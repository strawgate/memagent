impl OutputHealthTracker {
    pub(super) fn new(outputs: Vec<Arc<ComponentStats>>) -> Self {
        Self {
            outputs,
            state: std::sync::Mutex::new(OutputHealthState {
                worker_slots: HashMap::new(),
                idle_health: ComponentHealth::Healthy,
            }),
        }
    }

    fn publish(&self, health: ComponentHealth) {
        for stats in &self.outputs {
            stats.set_health(health);
        }
    }

    fn aggregate(state: &OutputHealthState) -> ComponentHealth {
        aggregate_output_health(state.idle_health, state.worker_slots.values().copied())
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, OutputHealthState> {
        self.state.lock().unwrap_or_else(|poisoned| {
            tracing::error!("worker_pool: recovering output health tracker after mutex poison");
            poisoned.into_inner()
        })
    }

    fn insert_worker(&self, worker_id: usize, initial: ComponentHealth) -> ComponentHealth {
        let mut state = self.lock_state();
        state.idle_health = idle_health_after_worker_insert(state.idle_health);
        state.worker_slots.insert(worker_id, initial);
        let aggregate = Self::aggregate(&state);
        self.publish(aggregate);
        aggregate
    }

    pub(super) fn apply_worker_event(
        &self,
        worker_id: usize,
        event: OutputHealthEvent,
    ) -> ComponentHealth {
        let mut state = self.lock_state();
        let Some(current) = state.worker_slots.get(&worker_id).copied() else {
            let aggregate = Self::aggregate(&state);
            tracing::warn!(
                worker_id,
                ?event,
                "worker_pool: ignoring output health event for unknown worker slot"
            );
            return aggregate;
        };
        let next = reduce_worker_slot_health(current, event);
        state.worker_slots.insert(worker_id, next);
        let aggregate = Self::aggregate(&state);
        self.publish(aggregate);
        aggregate
    }

    pub(super) fn remove_worker(&self, worker_id: usize) -> ComponentHealth {
        let mut state = self.lock_state();
        state.worker_slots.remove(&worker_id);
        let aggregate = Self::aggregate(&state);
        self.publish(aggregate);
        aggregate
    }

    fn has_active_workers(&self) -> bool {
        !self.lock_state().worker_slots.is_empty()
    }

    fn set_pool_health(&self, health: ComponentHealth) {
        let mut state = self.lock_state();
        state.idle_health = health;
        let aggregate = Self::aggregate(&state);
        self.publish(aggregate);
    }

    fn clear_workers_and_set_pool_health(&self, health: ComponentHealth) -> ComponentHealth {
        let mut state = self.lock_state();
        state.worker_slots.clear();
        state.idle_health = health;
        let aggregate = Self::aggregate(&state);
        self.publish(aggregate);
        aggregate
    }

    #[cfg(test)]
    fn slot_health(&self, worker_id: usize) -> Option<ComponentHealth> {
        self.state
            .lock()
            .expect("output health tracker mutex poisoned during test slot lookup")
            .worker_slots
            .get(&worker_id)
            .copied()
    }
}

// ---------------------------------------------------------------------------
// OutputWorkerPool
// ---------------------------------------------------------------------------
