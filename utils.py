import datetime,time,re,validators
import requests
import json
import constants
from urlparse import urlparse

# Ported by Matt Sullivan http://sullerton.com/2011/03/django-mobile-browser-detection-middleware/

reg_b = re.compile(r"(android|bb\\d+|meego).+mobile|avantgo|bada\\/|blackberry|blazer|compal|ipad|iphone|iphone os|ipod|elaine|fennec|hiptop|iemobile|ip(hone|od)|iris|kindle|lge |maemo|midp|mmp|mobile.+firefox|netfront|opera m(ob|in)i|palm( os)?|phone|p(ixi|re)\\/|plucker|pocket|psp|series(4|6)0|symbian|treo|up\\.(browser|link)|vodafone|wap|windows ce|xda|xiino", re.I|re.M)
reg_v = re.compile(r"1207|6310|6590|3gso|4thp|50[1-6]i|770s|802s|a wa|abac|ac(er|oo|s\\-)|ai(ko|rn)|al(av|ca|co)|amoi|an(ex|ny|yw)|aptu|ar(ch|go)|as(te|us)|attw|au(di|\\-m|r |s )|avan|be(ck|ll|nq)|bi(lb|rd)|bl(ac|az)|br(e|v)w|bumb|bw\\-(n|u)|c55\\/|capi|ccwa|cdm\\-|cell|chtm|cldc|cmd\\-|co(mp|nd)|craw|da(it|ll|ng)|dbte|dc\\-s|devi|dica|dmob|do(c|p)o|ds(12|\\-d)|el(49|ai)|em(l2|ul)|er(ic|k0)|esl8|ez([4-7]0|os|wa|ze)|fetc|fly(\\-|_)|g1 u|g560|gene|gf\\-5|g\\-mo|go(\\.w|od)|gr(ad|un)|haie|hcit|hd\\-(m|p|t)|hei\\-|hi(pt|ta)|hp( i|ip)|hs\\-c|ht(c(\\-| |_|a|g|p|s|t)|tp)|hu(aw|tc)|i\\-(20|go|ma)|i230|iac( |\\-|\\/)|ibro|idea|ig01|ikom|im1k|inno|ipaq|iris|ja(t|v)a|jbro|jemu|jigs|kddi|keji|kgt( |\\/)|klon|kpt |kwc\\-|kyo(c|k)|le(no|xi)|lg( g|\\/(k|l|u)|50|54|\\-[a-w])|libw|lynx|m1\\-w|m3ga|m50\\/|ma(te|ui|xo)|mc(01|21|ca)|m\\-cr|me(rc|ri)|mi(o8|oa|ts)|mmef|mo(01|02|bi|de|do|t(\\-| |o|v)|zz)|mt(50|p1|v )|mwbp|mywa|n10[0-2]|n20[2-3]|n30(0|2)|n50(0|2|5)|n7(0(0|1)|10)|ne((c|m)\\-|on|tf|wf|wg|wt)|nok(6|i)|nzph|o2im|op(ti|wv)|oran|owg1|p800|pan(a|d|t)|pdxg|pg(13|\\-([1-8]|c))|phil|pire|pl(ay|uc)|pn\\-2|po(ck|rt|se)|prox|psio|pt\\-g|qa\\-a|qc(07|12|21|32|60|\\-[2-7]|i\\-)|qtek|r380|r600|raks|rim9|ro(ve|zo)|s55\\/|sa(ge|ma|mm|ms|ny|va)|sc(01|h\\-|oo|p\\-)|sdk\\/|se(c(\\-|0|1)|47|mc|nd|ri)|sgh\\-|shar|sie(\\-|m)|sk\\-0|sl(45|id)|sm(al|ar|b3|it|t5)|so(ft|ny)|sp(01|h\\-|v\\-|v )|sy(01|mb)|t2(18|50)|t6(00|10|18)|ta(gt|lk)|tcl\\-|tdg\\-|tel(i|m)|tim\\-|t\\-mo|to(pl|sh)|ts(70|m\\-|m3|m5)|tx\\-9|up(\\.b|g1|si)|utst|v400|v750|veri|vi(rg|te)|vk(40|5[0-3]|\\-v)|vm40|voda|vulc|vx(52|53|60|61|70|80|81|83|85|98)|w3c(\\-| )|webc|whit|wi(g |nc|nw)|wmlb|wonu|x700|yas\\-|your|zeto|zte\\-", re.I|re.M)



def if_null(var, val):
  if var is None:
    return val
  return var

#validation of 1st column = date
def validate_date(date_column):
    result = False
    try:
        datetime.datetime.strptime(date_column, '%Y-%m-%d')
        result = True
    except ValueError:
        return result
        #raise ValueError("Incorrect data format, should be YYYY-MM-DD")
    return result

assert validate_date('2014-10-0s') == False
assert validate_date('2014-10-02') == True
assert validate_date('2-014-10-02') == False


#validation of 2nd column = time
def validate_time(time_column):
    result = False
    try:
        time.strptime(time_column, '%H:%M:%S')
        result = True
    except ValueError:
        return result
    return result

assert validate_time('00:01:02') == True
assert validate_time('0s0:01:02') == False
assert validate_time('00:0:001:02') == False
assert validate_time('28:01:02') == False
assert validate_time('23:51:02') == True


#Validation of 3rd column = url
def validate_url(url_column):

    #using module parse the hashed url
    parsed_url  = urlparse(url_column)

    #construct regex for each of the three pieces of the url -  http://hashed_domain/hashed_path
    regex_scheme = re.compile(r"(http|https)")
    regex_domain = re.compile(r"^[a-zA-Z0-9]+$")
    regex_path = re.compile(r"^(/)[a-zA-Z0-9]+$")

    #verify each piece
    match_scheme = regex_scheme.match(parsed_url.scheme)
    match_domain = regex_domain.match(parsed_url.netloc)
    match_path = regex_path.match(parsed_url.path)

    if match_domain and match_path and match_scheme:
        return True
    else:
        return False


assert validate_url('http://7c962b10ecce30b0990e298409c9dd786e163a79/9aa7adcc1843d36a9035f86122f36500ca2d11dc') == True
assert validate_url('http:////7c962b10ec////ce30b0990e298409c9dd786e163a79/9aa7adcc1843d36a9035f86122f36500ca2d11dc') == False
assert validate_url('http://7c962b10ecce30b0990e298409c9dd786e163a79/9aa7adcc1843d36a9035f86122f36500ca2d11dc///') == False
assert validate_url('http://7c962b10ecce30b0990e298409c9dd786e163a79/9aa7adcc1843d36a9035f86122f36500ca2d11dc///') == False
assert validate_url('https:/w') == False
assert validate_url('https://c962b10ecce30b0990e298409c9dd786e163a79/9aa7adcc1843d36a9') == True
assert validate_url('aaaa://w') == False
assert validate_url('https:////') == False



#Validation and processing of 4th column = ip
def validate_ip(ip_column):
    if validators.ip_address.ipv4(ip_column):
        return True
    else:
        return False

assert validate_ip('abc') == False
assert validate_ip('abc.0.0.0') == False
assert validate_ip('213.153.11.107') == True
assert validate_ip('....') == False
assert validate_ip('1111.0.0.0') == False
assert validate_ip('0.0.0.0') == True
assert validate_ip('11111111110.0.0.0') == False
assert validate_ip('213.153.11.10722333333333333') == False
assert validate_ip('.....................0.0.0...0') == False

#Offline database would be the ideal part here , http://blog.brush.co.nz/2009/07/geoip/ http://stackoverflow.com/questions/19514749/best-ip-to-country-database
def process_geolocation_data(ip_column):

    location={}
    #1.
    #STATIC_URL = 'http://freegeoip.net/json'
    #request_url = '{}/{}'.format(STATIC_URL, ip_column)
    #response = requests.get(request_url)
    #print(response.json())
    #2. API key = a24bab79e02ae0e4083b9327dc2c49a9f76babbac85486d99306fa0e18110f95   - might need to retest IP allowed in AWS
    request_url = constants.GEOLOCATIONURL + constants.APIKEY + '&ip=' + ip_column + '&format=json'
    response_content = requests.get(request_url)._content
    response_content_json = json.loads(response_content)
    location['latitude'] = if_null(response_content_json['latitude'],0.0)
    location['longitude'] = if_null(response_content_json['longitude'],0.0)
    location['country'] = if_null(response_content_json['countryName'],0.0)
    location['city'] = if_null(response_content_json['cityName'],0.0)
    return location


assert process_geolocation_data('213.153.11.107') == {'latitude': u'60.2551', 'country': u'Norway', 'longitude': u'5.10161', 'city': u'Skogsvagen'}
#assert process_geolocation_data('81.155.236.202') == {'latitude': u'51.5095', 'country': u'United Kingdom', 'longitude': u'-0.19576', 'city': u'Notting Hill Gate'}

#processing of 5th column = ua
def process_user_agent(ua_column):

    result = {}
    try:
        #construct GET request and load to JSON
        request_url = 'http://useragentstring.com/?uas=' + ua_column.replace(' ','%20') + '&getJSON=all'
        response_content = requests.get(request_url)._content
        response_content_json = json.loads(response_content)
        #print(response_content_json)
        #get is_mobile
        b = reg_b.search(ua_column)
        v = reg_v.search(ua_column[0:4])
        if b or v:
            result['mobile'] = True
        else:
            result['mobile'] = False

        #if response_content_json['os_name'].lower() in constants.OS_MOBILE or 'Mobile' in ua_column:
        #    result['is_mobile'] = True
        #else:
        #    result['is_mobile'] = False

        #get OS_family
        result['os_family'] = if_null(response_content_json['os_type'],'')
        #get full string
        result['string'] = ua_column
        #get browser_family
        result['browser_family'] = if_null(response_content_json['agent_name'],'')

    except ValueError:
        return result
    return result


assert (process_user_agent('Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.124 Safari/537.36')) ==\
       {'mobile': False, 'string': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.124 Safari/537.36',  'os_family': u'Windows', 'browser_family': u'Chrome'}
assert (process_user_agent('Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MTC19T) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.81 Mobile Safari/537.36')) ==\
       {'mobile': True, 'string': 'Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MTC19T) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.81 Mobile Safari/537.36', 'os_family': u'Android', 'browser_family': u'Android Webkit Browser'}
assert (process_user_agent('Mozilla/5.0 (iPad; CPU OS 7_1_2 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like Gecko) Version/7.0 Mobile/11D257 Safari/9537.53')) ==\
       {'mobile': True, 'string': 'Mozilla/5.0 (iPad; CPU OS 7_1_2 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like Gecko) Version/7.0 Mobile/11D257 Safari/9537.53', 'os_family': u'Macintosh', 'browser_family': u'Safari'}
assert (process_user_agent('Mozilla/5.0 (iPhone; CPU OS 7_1_2 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like Gecko) Version/7.0 Mobile/11D257 Safari/9537.53')) ==\
       {'mobile': True, 'string': 'Mozilla/5.0 (iPhone; CPU OS 7_1_2 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like Gecko) Version/7.0 Mobile/11D257 Safari/9537.53', 'os_family': u'Macintosh', 'browser_family': u'Safari'}
assert (process_user_agent('Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:24.0) Gecko/20100101 Firefox/24.0')) ==\
       {'mobile': False, 'string': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:24.0) Gecko/20100101 Firefox/24.0', 'os_family': u'Linux', 'browser_family': u'Firefox'}
