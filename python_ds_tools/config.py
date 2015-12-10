import os
import yaml
#import addict

folder = os.environ['ROOT_FOLDER']
name = 'config.yaml'
path = "%s/%s" % (folder, name)
f = open(path, 'r')
text = f.read()
dic = yaml.load(text)


main = dic

#main = addict.Dict(dic)

#http://stackoverflow.com/questions/4984647/accessing-dict-keys-like-an-attribute-in-python
# class AttrDict(dict):
#     def __init__(self, *args, **kwargs):
#         super(AttrDict, self).__init__(*args, **kwargs)
#         self.__dict__ = self

# class Main:
#     def __init__(self):
#         folder = os.environ['ROOT_FOLDER']
#         name = 'config.yaml'
#         path = "%s/%s" % (folder, name)
#         with open(path, 'r') as f:
#             text = f.read()
#             dic = yaml.load(text)
#             self.attr = AttrDict(dic)


#This script loads the config file in:
#ROOT_FOLDER/config.yaml
#and parses it
#Then, in every python file the data is accesible
#When using bash files, call python and uses this script to get the values
#That way you don't need to introduce path references in each file