def get_database_info(host):
    if '57' in host:
        # HOST
        host = "http://192.168.1.57:8515"
        # # # # 数据库连接信息
        MySQL_CONFIG = {
            'HOST': '192.168.1.57',
            "PORT": 3306,
            "USER": 'merce',
            "PASSWORD": 'merce',
            "DB": 'merce'
        }
        return host, MySQL_CONFIG

    elif '83' in host:
        # HOST
        host = "http://192.168.1.83:8515"
        # 数据库的连接配置，需要根据不同环境进行变更
        MySQL_CONFIG = {
            'HOST': '192.168.1.199',
            "PORT": 3306,
            "USER": 'merce',
            "PASSWORD": '123456',
            "DB": 'wac666',
            'case_db': 'test'}
        return host, MySQL_CONFIG

    else:
        print('请确认host信息')