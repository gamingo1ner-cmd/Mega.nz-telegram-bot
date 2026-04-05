blocked_ips = set()
ip_counter = {}

def check_ip(ip):

    count = ip_counter.get(ip,0) + 1
    ip_counter[ip] = count

    if count > 50:
        blocked_ips.add(ip)

    return ip not in blocked_ips
