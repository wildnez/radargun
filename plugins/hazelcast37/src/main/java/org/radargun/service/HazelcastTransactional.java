package org.radargun.service;

import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionOptions;
import org.radargun.traits.Transactional;

/**
 * Provides transactional operations for Hazelcast
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class HazelcastTransactional implements Transactional {
   protected final HazelcastService service;
   protected String CACHE_NAME;

   public HazelcastTransactional(HazelcastService service) {
      this.service = service;
   }

   @Override
   public Configuration getConfiguration(String cacheName) {
      this.CACHE_NAME = cacheName;
      return Configuration.TRANSACTIONS_ENABLED;
   }

   @Override
   public Transaction getTransaction() {
      return new Tx();
   }

   protected class Tx implements Transaction {

      private final TransactionContext context;

      private final com.hazelcast.core.TransactionalMap map;

      public Tx() {
         TransactionOptions options = new TransactionOptions()
                 .setTransactionType( TransactionOptions.TransactionType.ONE_PHASE );

         context = service.hazelcastInstance.newTransactionContext(options);

         map = context.getMap(CACHE_NAME);
         //this.tx = service.hazelcastInstance.getTransaction();
      }

      @Override
      public <T> T wrap(T resource) {
         return resource;
      }

      @Override
      public void begin() {
         context.beginTransaction();
      }

      @Override
      public void commit() {
         context.commitTransaction();
      }

      @Override
      public void rollback() {
         context.rollbackTransaction();
      }
   }
}
