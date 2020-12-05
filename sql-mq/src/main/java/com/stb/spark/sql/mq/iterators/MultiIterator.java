package com.stb.spark.sql.mq.iterators;

import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

public class MultiIterator<T> implements Iterator<T> {

    Set<Iterator<T>> baseIterator;

    public MultiIterator() {
        this.baseIterator = new HashSet<>();
    }

    public void add(Iterator<T> iterator){
        baseIterator.add(iterator);
    }

    @Override
    public boolean hasNext() {
        Set<Iterator<T>> toRemove = new HashSet<>();
        boolean hasNext = false;
        for(Iterator<T> i: baseIterator){
            hasNext = i.hasNext();
            if(hasNext){
                break;
            }else {
                toRemove.add(i);
            }
        }
        baseIterator.removeAll(toRemove);
        return hasNext;
    }

    @Override
    public T next() {
        if(hasNext()){
            return baseIterator.stream()
                    .findFirst().orElseThrow(NoSuchElementException::new).next();
        }
        return (T) new NoSuchElementException();
    }
}
