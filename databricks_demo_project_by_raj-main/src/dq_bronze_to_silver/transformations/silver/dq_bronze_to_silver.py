"""
Metadata-driven data quality: <catalog>.1_bronze -> <catalog>.2_silver

The catalog is passed as the pipeline parameter `catalog_name` (e.g. dev / prod). It is used
for the rules table (<catalog>.configs.dq_rules), the bronze sources and the silver targets,
so the same code and metadata can be promoted across environments.

At pipeline start, active rules are read from <catalog>.configs.dq_rules and grouped per
source/target table. create_silver_table() is called once per group and defines one
silver dataset with the rules applied as expectations (rule_name = expectation name):
  * action 'drop' -> expect_all_or_drop (failing rows removed)
  * action 'warn' -> expect_all        (rows kept, metric recorded)
  * action 'fail' -> expect_all_or_fail (update fails on violation)

Quarantine: for every table with at least one 'drop' rule, the rows removed from silver are
written to <catalog>.<target_schema>.<target_table>_quarantine with the extra columns
_dq_failed_rules (array of failed drop rule names), _dq_source_table and _dq_quarantined_at.
Drop rules are evaluated as coalesce(expr, false) in both datasets, so a NULL result counts as
a failure and every source row lands in exactly one of silver / quarantine.

read_mode 'stream' -> streaming table (incremental from append-only bronze Delta table)
read_mode 'batch'  -> materialized view (recomputed from bronze streaming tables, e.g. SCD2)

To onboard a new table or rule, insert a row into <catalog>.configs.dq_rules - no code change needed.
Rules whose expression cannot be resolved against the source table (syntax error, unknown
column, non-boolean result) are skipped with a warning so one bad rule does not block the
whole pipeline; fix the row in dq_rules and rerun.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType

CATALOG_PARAM = "catalog_name"
CATALOG = spark.conf.get(CATALOG_PARAM, "").strip()
if not CATALOG:
    raise ValueError(f"Pipeline parameter '{CATALOG_PARAM}' is required (set it in the pipeline configuration).")
QUARANTINE_SUFFIX = "_quarantine"
ACTION_TO_EXPECTATION = {
    "warn": dp.expect_all,
    "drop": dp.expect_all_or_drop,
    "fail": dp.expect_all_or_fail,
}


def quote_identifier(name):
    """Backtick-quote an identifier (required for schemas like 1_bronze / 2_silver)."""
    return "`" + name.replace("`", "``") + "`"


def fully_qualified_name(catalog, schema, table):
    return ".".join(quote_identifier(part) for part in (catalog, schema, table))


DQ_RULES_TABLE = fully_qualified_name(CATALOG, "configs", "dq_rules")


def load_table_configs():
    """Read active rules and group them by source table / target table."""
    rows = (
        spark.read.table(DQ_RULES_TABLE)
        .where(F.col("is_active") == F.lit(True))
        .select(
            "rule_id", "source_catalog", "source_schema", "source_table",
            "target_schema", "target_table", "read_mode", "column_name",
            "rule_name", "rule_expression", "action", "description",
        )
        .orderBy("source_schema", "source_table", "target_table", "rule_id")
        .collect()
    )

    configs = {}
    for r in rows:
        key = (r.source_schema, r.source_table, r.target_schema, r.target_table)
        if key not in configs:
            configs[key] = {
                "table_config": {
                    "source_catalog": CATALOG,  # catalog comes from the pipeline parameter
                    "source_schema": r.source_schema,
                    "source_table": r.source_table,
                    "target_catalog": CATALOG,
                    "target_schema": r.target_schema,
                    "target_table": r.target_table,
                    "read_mode": (r.read_mode or "batch").strip().lower(),
                },
                "rules": [],
            }
        configs[key]["rules"].append(
            {
                "rule_id": r.rule_id,
                "column_name": r.column_name,
                "rule_name": r.rule_name,
                "rule_expression": r.rule_expression,
                "action": (r.action or "").strip().lower(),
                "description": r.description,
            }
        )
    return list(configs.values())


def is_valid_rule(source_fqn, rule):
    """Check that the rule expression resolves to a boolean against the source schema."""
    try:
        schema = spark.read.table(source_fqn).select(F.expr(rule["rule_expression"]).alias("_chk")).schema
        return isinstance(schema["_chk"].dataType, BooleanType)
    except Exception as e:  # analysis/parse errors
        print(f"[dq] WARNING: skipping rule_id={rule['rule_id']} '{rule['rule_name']}' on {source_fqn}: "
              f"invalid expression {rule['rule_expression']!r} ({type(e).__name__})")
        return False


def null_safe(expression):
    """Treat a NULL rule result as a failure so silver and quarantine never overlap or miss rows."""
    return f"coalesce(({expression}), false)"


def create_silver_table(table_config, rules):
    """Define one silver dataset with valid records (and its quarantine table) for a source/target pair."""
    source_fqn = fully_qualified_name(
        table_config["source_catalog"], table_config["source_schema"], table_config["source_table"]
    )
    target_fqn = fully_qualified_name(
        table_config["target_catalog"], table_config["target_schema"], table_config["target_table"]
    )
    is_stream = table_config["read_mode"] == "stream"

    # Bucket expectations by action; rule_name is the expectation name.
    expectations = {action: {} for action in ACTION_TO_EXPECTATION}
    for rule in rules:
        action = rule["action"]
        if action not in ACTION_TO_EXPECTATION:
            print(f"[dq] WARNING: skipping rule_id={rule['rule_id']} on {source_fqn}: unknown action {action!r}")
            continue
        if not is_valid_rule(source_fqn, rule):
            continue
        name = rule["rule_name"]
        if any(name in bucket for bucket in expectations.values()):
            name = f"{name}_{rule['rule_id']}"  # expectation names must be unique per dataset
        expression = rule["rule_expression"]
        expectations[action][name] = null_safe(expression) if action == "drop" else expression

    def read_source():
        if is_stream:
            return spark.readStream.table(source_fqn)
        return spark.read.table(source_fqn)

    dataset_fn = read_source
    for action, decorator in ACTION_TO_EXPECTATION.items():
        if expectations[action]:
            dataset_fn = decorator(expectations[action])(dataset_fn)

    comment = (
        f"Silver {table_config['target_table']}: valid records from {source_fqn} "
        f"with DQ rules from {DQ_RULES_TABLE} ({'incremental stream' if is_stream else 'batch recompute'})."
    )
    if is_stream:
        dp.table(name=target_fqn, comment=comment)(dataset_fn)
    else:
        dp.materialized_view(name=target_fqn, comment=comment)(dataset_fn)

    # Quarantine: rows failing at least one drop rule, with the list of rules they failed.
    drop_rules = expectations["drop"]
    if not drop_rules:
        return

    quarantine_fqn = fully_qualified_name(
        table_config["target_catalog"],
        table_config["target_schema"],
        table_config["target_table"] + QUARANTINE_SUFFIX,
    )
    failed_rules_col = F.filter(
        F.array(*[F.when(~F.expr(expr), F.lit(name)) for name, expr in drop_rules.items()]),
        lambda x: x.isNotNull(),
    )

    def read_quarantine():
        return (
            read_source()
            .withColumn("_dq_failed_rules", failed_rules_col)
            .where(F.size("_dq_failed_rules") > 0)
            .withColumn("_dq_source_table", F.lit(source_fqn))
            .withColumn("_dq_quarantined_at", F.current_timestamp())
        )

    quarantine_comment = (
        f"Quarantine for {target_fqn}: records from {source_fqn} dropped by DQ rules "
        f"({', '.join(drop_rules)})."
    )
    if is_stream:
        dp.table(name=quarantine_fqn, comment=quarantine_comment)(read_quarantine)
    else:
        dp.materialized_view(name=quarantine_fqn, comment=quarantine_comment)(read_quarantine)


for cfg in load_table_configs():
    create_silver_table(cfg["table_config"], cfg["rules"])
