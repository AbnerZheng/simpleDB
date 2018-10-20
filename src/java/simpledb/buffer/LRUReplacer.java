package simpledb.buffer;

import java.util.HashMap;
import java.util.Map;

/**
 * Create by hzzhenglu on 2018/10/20
 */

public class LRUReplacer<T> implements Replacer<T> {
	private DLinkedNode head;

	private DLinkedNode tail;
	private Map<T, DLinkedNode> map;

	public LRUReplacer(){
		this.head = new DLinkedNode();
		this.tail = new DLinkedNode();
		this.head.next = this.tail;
		this.head.pre = null;
		this.tail.pre  = this.head;
		this.tail.next = null;

		this.map = new HashMap<>();
	}


	@Override
	public void insert(final T value) {
		DLinkedNode dLinkedNode = this.map.get(value);
		if(dLinkedNode != null){
			dLinkedNode.pre.next = dLinkedNode.next;
			dLinkedNode.next.pre = dLinkedNode.pre;
		}else {
			dLinkedNode = new DLinkedNode();
			dLinkedNode.value = value;
			this.map.put(value, dLinkedNode);
		}
		this.head.next.pre = dLinkedNode;
		dLinkedNode.next = this.head.next;
		dLinkedNode.pre  = this.head;
		this.head.next = dLinkedNode;
	}

	@Override
	public T victim() {
		if(this.map.isEmpty()){
			return null;
		}
		DLinkedNode cur = this.tail.pre;
		this.map.remove(cur.value);
		cur.pre.next = cur.next;
		cur.next.pre = cur.pre;
		return cur.value;
	}

	@Override
	public boolean erase(final T value) {
		return false;
	}

	@Override
	public int size() {
		return map.size();
	}
	private class DLinkedNode{
		T value;
		DLinkedNode pre;
		DLinkedNode next;
	}
}
