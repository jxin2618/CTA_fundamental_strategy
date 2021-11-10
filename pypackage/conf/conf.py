# -*- coding: utf-8 -*-
"""
Created on Tue Jul 14 13:29:29 2020

@author: DamonChan
"""

from configparser import ConfigParser

class Conf(ConfigParser):  
    def __init__(self,defaults=None):  
        ConfigParser.__init__(self,defaults=None)  
    def optionxform(self, optionstr):  
        return optionstr


# EOF
