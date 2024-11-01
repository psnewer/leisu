# -*- coding: utf-8 -*-

from manager.processor import Processor
from conf import *


if __name__ == "__main__":
	Flags(sys.argv)
	pro = Processor()
	if (gflags.FLAGS.extract):
		pro.extract()
	if (gflags.FLAGS.feature):
		pro.feature()
	if (gflags.FLAGS.filter):
		pro.filter()
	if (gflags.FLAGS.test):
		pro.test()
	if (gflags.FLAGS.analysis):
		pro.analysis()
	if (gflags.FLAGS.predict):
		pro.predict()

	pro.close()
