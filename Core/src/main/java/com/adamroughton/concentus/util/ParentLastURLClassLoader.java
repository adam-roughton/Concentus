package com.adamroughton.concentus.util;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParentLastURLClassLoader extends URLClassLoader {

	private final ClassLoader _systemClassLoader;
	private final Set<String> _passThroughSet;
	
	private final Logger _logger = LoggerFactory.getLogger(ParentLastURLClassLoader.class);
	
	public ParentLastURLClassLoader(URL[] urls, String[] passThroughList, ClassLoader parent) {
		super(urls, parent);
		
		_passThroughSet = new HashSet<>(passThroughList.length);
		for (String className : passThroughList) {
			_passThroughSet.add(className);
		}
		
		ClassLoader systemClassLoader = ClassLoader.getSystemClassLoader();
		while (systemClassLoader.getParent() != null) 
			systemClassLoader = systemClassLoader.getParent();
		_systemClassLoader = systemClassLoader;
	}

	@Override
	public void addURL(URL url) {
		super.addURL(url);
	}

	@Override
	protected Class<?> loadClass(String name, boolean resolve)
			throws ClassNotFoundException {
		_logger.trace("Looking for class {}", name);
		// return previously loaded classes from this class loader
		Class<?> klass = findLoadedClass(name);		
		if (klass == null) {
			_logger.trace("Class {} not previously loaded, looking in the system class loader", name);
			try {
				klass = _systemClassLoader.loadClass(name);
			} catch (ClassNotFoundException eNotFound) {
				// ignore
			}
			if (klass == null) {
				if (_passThroughSet.contains(name)) {
					_logger.trace("Class {} in the pass through list - delegating to parent", name);
					// class is on the pass-through list: delegate to the parent class loader
					klass = super.loadClass(name, resolve);
				} else {
					_logger.trace("Class {} not in the system class loader, looking in this class loader", name);
					try {
						// look in the URLs of this class loader first
						klass = findClass(name);
					} catch (ClassNotFoundException eNotFound) {
						_logger.trace("Class {} not found in this class loader; delegating to parent", name);
						// if not found, delegate to the parent class loader
						klass = super.loadClass(name, resolve);
					}
				}
			}
		}
		if (resolve) {
			resolveClass(klass);
		}
		_logger.trace("Returning class {}", name);
		return klass;
	}
	
}
