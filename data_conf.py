#!/usr/bin/python3

import json

class Conf():
    def __init__(self):
        fh = open('data.conf')
        self.__dict__ = json.load(fh)

    def change_field(self, fieldname, change):
        print_vals = (fieldname, self.__dict__[fieldname], change)
        print('Changing %s in data.conf from %s to %s' % print_vals)

        fh = open('data.conf', 'r')
        conf = json.load(fh)
        conf[fieldname] = change
        self.__dict__ = conf
        fh.close()

        fh = open('data.conf', 'w')
        json.dump(conf, fh, indent=4)
        fh.close()

data_conf = Conf()
