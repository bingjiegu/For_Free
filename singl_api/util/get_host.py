from urllib.parse import urlparse

def get_host(url):
    host = urlparse(url).scheme + '://' + urlparse(url).netloc
    return host

url = 'http://192.168.1.57:8515/api/datasets/name/'
print(get_host(url), type(get_host(url)))