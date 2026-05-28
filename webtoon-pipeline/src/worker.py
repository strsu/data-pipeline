import faust
from src.config.settings import FAUST_APP_NAME, KAFKA_BROKERS

app = faust.App(
    FAUST_APP_NAME,
    broker=[f"kafka://{b}" for b in KAFKA_BROKERS],
    topic_replication_factor=1,
    autodiscover=["src.agents"],
    origin="src",
    consumer_auto_offset_reset="latest",
    topic_allow_declare=False,
)

if __name__ == "__main__":
    app.main()
