package projection

// loadChildren is the single ownership boundary for inline and queued loads.
// Only one pass can load a selector generation at a time. Joiners wait without
// holding actor locks; a changed selector has a different generation.
//
// New interests remain private until every current child slot is covered. If a
// retry removes a sibling during IO, retain the work already loaded and fill
// only that gap. Completion and ownership validation share processMu, so an old
// pass cannot attach to a rebound parent or hide a newer recovery request.
func (p *Projection) loadChildren(expansion projectionExpansion) ([]string, error) {
	generation := expansion.member.expansionToken
	var done chan struct{}
	for done == nil {
		p.processMu.Lock()
		p.mu.Lock()
		if !p.graph.expansionCurrentLocked(expansion) {
			p.mu.Unlock()
			p.processMu.Unlock()
			return nil, nil
		}
		wait := generation.loading
		if wait == nil {
			done = make(chan struct{})
			generation.loading = done
		}
		p.mu.Unlock()
		p.processMu.Unlock()
		if wait != nil {
			select {
			case <-wait:
			case <-p.ctx.Done():
				return nil, p.ctx.Err()
			}
		}
	}
	defer func() {
		p.mu.Lock()
		generation.loading = nil
		close(done)
		p.mu.Unlock()
	}()

	var opened []string
	for {
		ids, err := p.openChildren(expansion, opened)
		opened = append(opened, ids...)
		p.processMu.Lock()
		p.mu.Lock()
		current := p.graph.expansionCurrentLocked(expansion)
		canceled := p.ctx.Err()
		complete := current && err == nil && canceled == nil && p.graph.attachExpansionLocked(expansion, opened)
		p.mu.Unlock()
		p.processMu.Unlock()
		if !current || err != nil || canceled != nil {
			for _, id := range opened {
				p.closeInstance(id)
			}
			if !current {
				return nil, nil
			}
			if canceled != nil {
				return nil, canceled
			}
			return nil, err
		}
		if complete {
			return opened, nil
		}
	}
}
