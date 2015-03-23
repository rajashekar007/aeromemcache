#This is to test teh functionalities supported by aerospike memcache proxy 
#Currently FLUSH and STATS are not supported
#to run this test, give python func_test.py in the terminal.

import pyermc
import random
import unittest
from datetime import datetime

class TestProxyEngineFunctionality(unittest.TestCase):
	G_key="G_KEY"
	G_value="G_Value"+str(datetime.now())

	def setUp(self):
		self.c=pyermc.Client(host='127.0.0.1',port=11211,disable_nagle=True,cache_cas=True,client_driver=pyermc.driver.binaryproto.BinaryProtoDriver)
		self.c.connect()
		self.c.delete(self.G_key)
       
	#set test
	def test_set(self):
		self.assertTrue(self.c.set(self.G_key,self.G_value))
		print "Set success"

	#get test
	def test_get(self):
		self.c.set(self.G_key,"get_value");
		self.assertEqual(self.c.get(self.G_key),"get_value")
		print "Get Success"

	#append test
	def test_append(self):
		self.c.set(self.G_key,"original_string")
		self.c.append(self.G_key,"_appended")
		self.assertEqual(self.c.get(self.G_key),"original_string_appended")
		print "append Success"

	#prepend test	
	def test_prepend(self):
		self.c.set(self.G_key,"original_string")
		self.c.prepend(self.G_key,"prepended_")
		self.assertEqual(self.c.get(self.G_key),"prepended_original_string")
		print "prepend success"

	#increment test
	def test_incr(self):
		self.c.set(self.G_key,123)
		self.c.incr(self.G_key,7)
		self.assertEqual(self.c.get(self.G_key),130)
		print "Increment Success"
	
	#decrement test
	def test_decr(self):
		self.c.set(self.G_key,123)
		self.c.decr(self.G_key,3)
		self.assertEqual(self.c.get(self.G_key),120)
		print "Decrement Success"

	#replace test
	def test_replace(self):
		self.c.set(self.G_key,"No")
		self.c.replace(self.G_key,"Yes")
		self.assertEqual(self.c.get(self.G_key),"Yes")
		print "Replace test Success"
	
	#add test
	def test_add(self):
		self.c.add(self.G_key,"add_value")
		self.assertEqual(self.c.get(self.G_key),"add_value")
		print "Add test Successful"

	#version test
	def test_version(self):
		print self.c.version()	
		print "Version test successful"
	
	#CAS(Check And Set) test
	def test_CAS(self):
		self.assertTrue(self.c.cas(self.G_key,"CAS_correction1",1))
		self.assertTrue(self.c.set(self.G_key,"CAS"))
		self.assertEqual(self.c.get(self.G_key),"CAS")
		print "CAS test successful"
		
if __name__ == '__main__':
    unittest.main()


