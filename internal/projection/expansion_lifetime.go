package projection

// currentInstance is the common delivery/failure routing boundary. The caller
// holds processMu so parent replacement and ownership reconciliation are one
// transition. Obsolete registered loads are retired before payload evaluation;
// published predecessors remain live until their normal replacement boundary.
func (p *Projection) currentInstance(id string) *projectionInstance {
	p.mu.Lock()
	instance := p.graph.instances[id]
	current := p.graph.instanceCurrentLocked(instance)
	p.mu.Unlock()
	if !current {
		if instance != nil {
			p.closeInstance(id)
		}
		return nil
	}
	return instance
}

// installInstanceLocked records initial-admission changes only after graph
// registration succeeds. Admission tracking is not graph ownership.
func (p *Projection) installInstanceLocked(instance *projectionInstance) error {
	if err := p.graph.installInstanceLocked(instance, p.limits.MaxActiveInstances); err != nil {
		return err
	}
	if p.initialChanges != nil {
		p.initialChanges.open(instance.id)
	}
	return nil
}
