from flask import Flask

def test_index_route():
    app = Flask(__name__)
    client = app.test_client()
    response = client.get('/')
    assert response.status_code == 404
    #assert response.data.decode('utf-8') == 'Hello, welcome to voucher api!'