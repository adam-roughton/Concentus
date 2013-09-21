package com.adamroughton.concentus.cluster;

import com.netflix.curator.framework.listen.Listenable;

public interface VersioningListenable<TListener> extends Listenable<TListener> {
		
	void resetListenerVersion(int version, TListener listener);
	
	int getListenerVersion(TListener listener);
	
}
