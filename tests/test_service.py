from app.service import produce_message, consume_message

def test_kafka_flow():
    produce_message()
    msg = consume_message()
    assert msg["msg"] == "hello world"
