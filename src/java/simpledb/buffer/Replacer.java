package simpledb.buffer;

import java.util.List;

/**
 * Create by hzzhenglu on 2018/10/20
 */

public interface Replacer<T> {
	void insert(T value);
	T victim();
	boolean erase(T value);
	int size();
}
