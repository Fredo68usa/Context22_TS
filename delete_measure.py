from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

def delete_inFlux():
     token = "KAhTOJpoan3nbxiTNPST5c2ZUstTpTbfspMKBKTO0HUYP4xfwnSuIAZxW6ty5mKYmkNDN9gqQGc1k8KB5xXjOw=="
     org = "context22"
     bucket = "context22"

     client = InfluxDBClient(url="http://sdcdiapplnx9160:8086", token=token)

     delete_api = client.delete_api()


     ret = delete_api.delete('1970-01-01T00:00:00Z', '2021-07-30T00:00:00Z', '_measurement="Query #104 Count of SQLS"', bucket="context22", org="context22")
     ret = delete_api.delete('1970-01-01T00:00:00Z', '2021-07-30T00:00:00Z', '_measurement="Query #104 Extractions"', bucket="context22", org="context22")
     ret = delete_api.delete('1970-01-01T00:00:00Z', '2021-07-30T00:00:00Z', '_measurement="Query #104 Resp. Time"', bucket="context22", org="context22")

     print ("Return code = ", ret )

if __name__ == '__main__':
    print(" Delete Measurement")


    delete_inFlux()

