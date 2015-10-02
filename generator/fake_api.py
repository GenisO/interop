# encoding: utf-8

def put_content(oauth, file_id, file_path):
    print "PUT %s %s %s" %(oauth, file_name, file_path)
    return 200

def get_content(oauth, file_id):
    print "GET %s %s" %(oauth, file_id, file_path)
    return 200

def make(oauth, name, is_folder=False):
    print "MAKE %s %s" %(oauth, name)
    return 200

def unlink(oauth, item_id, is_folder=False):
    print "DELETE %s %s %s" %(oauth, item_id, is_folder)
    return 200

def authenticate_request(username, password, client_key, client_secret):
    print "AUTHENTICATE %s %s %s %s" %(username, password, client_key, client_secret)
    return 200

def move(oauth, item_id, is_folder=False):
    print "DELETE %s %s %s" %(oauth, item_id, is_folder)
    return 200
