#!/usr/bin/env python2
import dropbox_exploit as dropbox

# custom size distr function
import random
def gauss(mu, sigma):
	while True:
		yield int(random.gauss(mu, sigma))

dropbox_example = dropbox.wrapper(instance_name="defcon21", APP_KEY = 'fhf5x0mwmzke0pn', APP_SECRET  = '8tkuc464y3m3t5o', chunk_size=gauss(64*1024,256))


test_file='core.py'

key = dropbox_example.put(test_file)
print (key)
dropbox_example.get(key)

