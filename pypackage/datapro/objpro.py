# -*- coding: utf-8 -*-
"""
Created on Mon Dec 11 18:45:05 2017

@author: ciaciaciu
"""

from .typepro import TypePro

class ObjPro(object):
    
    @classmethod
    def add_empty_object(cls, obj, name):
        """Add an empty object to ith name .
        
        Params:
        -------
        obj : python object
        name : str, list, series
        
        Returns:
        inplace
        
        """
        name = TypePro.to_list(name)
        for i in name:
            obj.__setattr__(i, EmptyObject())
    
    @classmethod
    def set_attr(cls, obj, data_dict):
        """Set attributes to object .
        
        Params:
        -------
        obj : python object
        data_dict : dict
            * keys are names of object attributes
            * values are values of object attributes
        
        Returns:
        --------
        inplace
        
        """
        for i in data_dict.keys():
            obj.__setattr__(i, data_dict[i])

    @classmethod
    def get_attr(cls, obj, name):
        """Get attributes from object
        
        Params:
        -------
        obj : python object
        name : str, list, series
        
        Returns:
        --------
        dict
        
        """
        name = TypePro.to_list(name)
        data = {}
        for i in name:
            try:
                data[i] = obj.__getattribute__(i).copy()
            except:
                data[i] = obj.__getattribute__(i)
        return data

class EmptyObject(object):
    """An empty empty object .
    """
    
    
# EOF