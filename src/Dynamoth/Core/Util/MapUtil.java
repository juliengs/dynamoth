package Dynamoth.Core.Util;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

public class MapUtil
{
	// INSPIRED FROM: http://stackoverflow.com/questions/109383/how-to-sort-a-mapkey-value-on-the-values-in-java
	
    public static <K, V> Map<K, V> 
        sortWithComparator( Map<K, V> map, Comparator<Map.Entry<K, V>> comparator )
    {
        List<Map.Entry<K, V>> list =
            new LinkedList<Map.Entry<K, V>>( map.entrySet() );
        Collections.sort( list, comparator );

        Map<K, V> result = new LinkedHashMap<K, V>();
        for (Map.Entry<K, V> entry : list)
        {
            result.put( entry.getKey(), entry.getValue() );
        }
        return result;
        
        /*
         * Collections.sort( list, new Comparator<Map.Entry<K, V>>()
        {
            public int compare( Map.Entry<K, V> o1, Map.Entry<K, V> o2 )
            {
                return (o1.getValue()).compareTo( o2.getValue() );
            }
        } );
         */
    }
    
    public static class CollectionReverser<T> implements Iterable<T> {
        private ListIterator<T> listIterator;        

        public CollectionReverser(Collection<T> wrappedCollection) {
            this.listIterator = new LinkedList<T>(wrappedCollection).listIterator(wrappedCollection.size());            
        }               

        public Iterator<T> iterator() {
            return new Iterator<T>() {

                public boolean hasNext() {
                    return listIterator.hasPrevious();
                }

                public T next() {
                    return listIterator.previous();
                }

                public void remove() {
                    listIterator.remove();
                }

            };
        }

    }
}