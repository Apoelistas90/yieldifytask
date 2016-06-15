import etl_functions

# validate date
assert etl_functions.validate_date('2014-10-0s') == False
assert etl_functions.validate_date('2014-10-02') == True
assert etl_functions.validate_date('2-014-10-02') == False

# validate time
assert etl_functions.validate_time('00:01:02') == True
assert etl_functions.validate_time('0s0:01:02') == False
assert etl_functions.validate_time('00:0:001:02') == False
assert etl_functions.validate_time('28:01:02') == False
assert etl_functions.validate_time('23:51:02') == True

# validate url
assert etl_functions.validate_url('http://7c962b10ecce30b0990e298409c9dd786e163a79/9aa7adcc1843d36a9035f86122f36500ca2d11dc') == True
assert etl_functions.validate_url('http:////7c962b10ec////ce30b0990e298409c9dd786e163a79/9aa7adcc1843d36a9035f86122f36500ca2d11dc') == False
assert etl_functions.validate_url('http://7c962b10ecce30b0990e298409c9dd786e163a79/9aa7adcc1843d36a9035f86122f36500ca2d11dc///') == False
assert etl_functions.validate_url('http://7c962b10ecce30b0990e298409c9dd786e163a79/9aa7adcc1843d36a9035f86122f36500ca2d11dc///') == False
assert etl_functions.validate_url('https:/w') == False
assert etl_functions.validate_url('https://c962b10ecce30b0990e298409c9dd786e163a79/9aa7adcc1843d36a9') == True
assert etl_functions.validate_url('aaaa://w') == False
assert etl_functions.validate_url('https:////') == False

# validate IP
assert etl_functions.validate_ip('abc') == False
assert etl_functions.validate_ip('abc.0.0.0') == False
assert etl_functions.validate_ip('213.153.11.107') == True
assert etl_functions.validate_ip('....') == False
assert etl_functions.validate_ip('1111.0.0.0') == False
assert etl_functions.validate_ip('0.0.0.0') == True
assert etl_functions.validate_ip('11111111110.0.0.0') == False
assert etl_functions.validate_ip('213.153.11.10722333333333333') == False
assert etl_functions.validate_ip('.....................0.0.0...0') == False

# process geolocation data
assert etl_functions.process_geolocation_data('213.153.11.107') == {'latitude': u'60.2551', 'country': u'Norway', 'longitude': u'5.10161', 'city': u'Skogsvagen'}
#assert wrapper.process_geolocation_data('81.155.236.202') == {'latitude': u'51.5095', 'country': u'United Kingdom', 'longitude': u'-0.19576', 'city': u'Notting Hill Gate'}

# process user agent
assert (etl_functions.process_user_agent('Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.124 Safari/537.36')) == \
       {'mobile': False, 'string': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.124 Safari/537.36',  'os_family': u'Windows', 'browser_family': u'Chrome'}
assert (etl_functions.process_user_agent('Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MTC19T) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.81 Mobile Safari/537.36')) == \
       {'mobile': True, 'string': 'Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MTC19T) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.81 Mobile Safari/537.36', 'os_family': u'Android', 'browser_family': u'Android Webkit Browser'}
assert (etl_functions.process_user_agent('Mozilla/5.0 (iPad; CPU OS 7_1_2 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like Gecko) Version/7.0 Mobile/11D257 Safari/9537.53')) == \
       {'mobile': True, 'string': 'Mozilla/5.0 (iPad; CPU OS 7_1_2 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like Gecko) Version/7.0 Mobile/11D257 Safari/9537.53', 'os_family': u'Macintosh', 'browser_family': u'Safari'}
assert (etl_functions.process_user_agent('Mozilla/5.0 (iPhone; CPU OS 7_1_2 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like Gecko) Version/7.0 Mobile/11D257 Safari/9537.53')) == \
       {'mobile': True, 'string': 'Mozilla/5.0 (iPhone; CPU OS 7_1_2 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like Gecko) Version/7.0 Mobile/11D257 Safari/9537.53', 'os_family': u'Macintosh', 'browser_family': u'Safari'}
assert (etl_functions.process_user_agent('Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:24.0) Gecko/20100101 Firefox/24.0')) == \
       {'mobile': False, 'string': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:24.0) Gecko/20100101 Firefox/24.0', 'os_family': u'Linux', 'browser_family': u'Firefox'}



