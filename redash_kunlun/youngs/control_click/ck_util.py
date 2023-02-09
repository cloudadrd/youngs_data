from clickhouse_driver import Client


class clickhouse_util:
    def __init__(self):
        self.host='10.0.29.66'
        self.port='9000'
        self.password='zSEA4XzCMKKV6rbA6fLy'
    def return_client(self):
        client = Client(host=self.host, port=self.port, database='real_bi', user='default',
                        password=self.password)

        return client
