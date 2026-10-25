from pyspark import pipelines as dp

@dp.table(
    name="gold_customer_360"
)
def gold_customer_360():

    customer = spark.read.table("silver_customer").alias("c")
    policy   = spark.read.table("silver_policy").alias("p")
    vehicle  = spark.read.table("silver_vehicle").alias("v")
    claim    = spark.read.table("silver_claim").alias("cl")
    agent    = spark.read.table("silver_agent").alias("a")

    return (
        customer
        .join(policy,
              customer.customer_id == policy.customer_id,
              "left")
        .join(vehicle,
              policy.policy_id == vehicle.policy_id,
              "left")
        .join(claim,
              policy.policy_id == claim.policy_id,
              "left")
        .join(agent,
              policy.agent_id == agent.agent_id,
              "left")
        .select(
            customer.customer_id,
            customer.customer_name,
            customer.city,
            customer.customer_tier,

            policy.policy_id,
            policy.premium_amount,
            policy.policy_status,

            vehicle.vehicle_id,
            vehicle.make,
            vehicle.model,

            claim.claim_id,
            claim.claim_type,
            claim.claim_amount,
            claim.claim_status,

            agent.agent_name,
            agent.region
        )
    )