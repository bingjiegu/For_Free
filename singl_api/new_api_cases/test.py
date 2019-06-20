from urllib.parse import urlparse


url = 'http://192.168.1.57:8515/api/flows/create'
host = urlparse(url).scheme + '://' + urlparse(url).netloc
print(host)
if '57' in host:
    print(1)