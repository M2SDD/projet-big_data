import re

def parse_log_line(line):
    """
    Parse une ligne de log et retourne un dictionnaire.
    Retourne None si la ligne ne correspond pas au format.
    """
    res = None
    # Regex Common Log Format (CLF)
    log_pattern = r'^(?P<ip>\S+) (?P<logname>\S+) (?P<user>\S+) (?P<ts>\[(.*?)\]) '
    log_pattern += r'"(?P<method>[A-Z]+) (?P<url>\S+) (?P<protocol>\S+)" '
    log_pattern += r'(?P<code>\d{3}) (?P<size>\d+|-)$'

    match = re.search(log_pattern, line)
    if match:
        res =  {
            "ip": match.group('ip'),
            "logname": match.group('logname'),
            "user": match.group('user'),
            "timestamp": match.group('ts'),
            "method": match.group('method'),
            "url": match.group('url'),
            "protocol": match.group('protocol'),
            "code": int(match.group('code')),
            "size": int(match.group('size'))
        }

    return res



if __name__ == '__main__':
    tests = [
        '192.168.0.168 - - [02/Feb/2025:15:19:50 +0000] "GET /products/eyeliner?id=10 HTTP/1.1" 403 11265',
        '192.168.0.96 - - [02/Feb/2025:15:19:50 +0000] "PUT /user/login HTTP/1.1" 301 14454',
        '192.168.2.25 - - [02/Feb/2025:15:19:50 +0000] "GET /products/skincare/cream?id=4 HTTP/1.1" 404 3961',
        '192.168.0.228 - - [02/Feb/2025:15:19:50 +0000] "OPTIONS /api/recommendations HTTP/1.1" 404 12587',
        '192.168.1.208 - - [02/Feb/2025:15:19:50 +0000] "PUT /products/eyeliner?id=4 HTTP/1.1" 200 1759',
        '192.168.1.81 - - [02/Feb/2025:15:19:50 +0000] "POST /products/lipstick?id=9 HTTP/1.1" 500 10282',
        '192.168.0.117 - - [02/Feb/2025:15:19:50 +0000] "OPTIONS /products/lipstick?id=10 HTTP/1.1" 404 12213',
        '192.168.2.120 - - [02/Feb/2025:15:19:50 +0000] "DELETE /products/eyeliner?id=4 HTTP/1.1" 404 1759',
        '192.168.3.156 - - [02/Feb/2025:15:19:50 +0000] "DELETE /user/register HTTP/1.1" 404 1707',
        '192.168.0.146 - - [02/Feb/2025:15:19:50 +0000] "GET /products/skincare/serum?id=4 HTTP/1.1" 301 10638',
    ]

    for test in tests:
        print(parse_log_line(test))