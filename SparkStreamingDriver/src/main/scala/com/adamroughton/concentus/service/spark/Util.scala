package com.adamroughton.concentus.service.spark

private case class ArgList(list: List[String]) {
	    
	def ::(condAndArg: (Boolean, String)): ArgList = {
		if (condAndArg._1) {
	    	new ArgList(condAndArg._2 :: list)
	    } else {
	    	this      
	    }
	}
     
	def :::(condAndArg: (Boolean, List[String])): ArgList = {
		if (condAndArg._1) {
			new ArgList(condAndArg._2 ::: list)
		} else {
			this      
		}
	}
     
	def ::(item: String) : ArgList = {
		new ArgList(item :: list)
	}
     
	def toList(): List[String] = {
		list
	}
}

  
