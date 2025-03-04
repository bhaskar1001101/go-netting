package main

import (
    "fmt"
    "math"
)

type Intent struct {
    Sender    string
    Receiver  string
    Token     string
    Amount    uint64
}

// Edge represents a directed edge in the graph with token and amount
type Edge struct {
    To     string
    Token  string
    Amount uint64
}

// Graph represents the debt network
type Graph struct {
    // map[from][]Edge
    Edges map[string][]Edge
}

func NewGraph() *Graph {
    return &Graph{
        Edges: make(map[string][]Edge),
    }
}

// Add or update edge in the graph
func (g *Graph) AddEdge(from, to, token string, amount uint64) {
    // Check if edge already exists
    for i, edge := range g.Edges[from] {
        if edge.To == to && edge.Token == token {
            g.Edges[from][i].Amount += amount
            return
        }
    }
    
    // Add new edge
    if _, exists := g.Edges[from]; !exists {
        g.Edges[from] = make([]Edge, 0)
    }
    g.Edges[from] = append(g.Edges[from], Edge{To: to, Token: token, Amount: amount})
}

// Tarjan's algorithm for finding SCCs
func (g *Graph) FindSCCs() [][]string {
    index := 0
    stack := make([]string, 0)
    onStack := make(map[string]bool)
    indices := make(map[string]int)
    lowlink := make(map[string]int)
    sccs := make([][]string, 0)

    var strongConnect func(v string)
    strongConnect = func(v string) {
        indices[v] = index
        lowlink[v] = index
        index++
        stack = append(stack, v)
        onStack[v] = true

        // Consider successors
        for _, edge := range g.Edges[v] {
            w := edge.To
            if _, exists := indices[w]; !exists {
                // Successor not yet visited
                strongConnect(w)
                lowlink[v] = int(math.Min(float64(lowlink[v]), float64(lowlink[w])))
            } else if onStack[w] {
                // Successor is on stack and hence in current SCC
                lowlink[v] = int(math.Min(float64(lowlink[v]), float64(indices[w])))
            }
        }

        // If v is a root node, pop the stack and generate an SCC
        if lowlink[v] == indices[v] {
            scc := make([]string, 0)
            for {
                w := stack[len(stack)-1]
                stack = stack[:len(stack)-1]
                onStack[w] = false
                scc = append(scc, w)
                if w == v {
                    break
                }
            }
            if len(scc) > 1 { // Only interested in SCCs with size > 1
                sccs = append(sccs, scc)
            }
        }
    }

    // Find SCCs
    for v := range g.Edges {
        if _, exists := indices[v]; !exists {
            strongConnect(v)
        }
    }

    return sccs
}

// Find cycles in a strongly connected component
func (g *Graph) FindCycles(scc []string, maxLength int) [][]string {
    cycles := make([][]string, 0)
    visited := make(map[string]bool)
    path := make([]string, 0)

    var findCyclesRecursive func(current string, start string, depth int)
    findCyclesRecursive = func(current string, start string, depth int) {
        if depth > maxLength {
            return
        }

        if depth > 0 && current == start {
            // Found a cycle
            cycleCopy := make([]string, len(path))
            copy(cycleCopy, path)
            cycles = append(cycles, cycleCopy)
            return
        }

        visited[current] = true
        path = append(path, current)

        for _, edge := range g.Edges[current] {
            if !visited[edge.To] || edge.To == start {
                findCyclesRecursive(edge.To, start, depth+1)
            }
        }

        path = path[:len(path)-1]
        visited[current] = false
    }

    // Start DFS from each vertex
    for _, v := range scc {
        findCyclesRecursive(v, v, 0)
    }

    return cycles
}

// Calculate netting amount for a cycle
func (g *Graph) CalculateNetting(cycle []string, token string) uint64 {
    minAmount := uint64(math.MaxUint64)

    // Find minimum amount in cycle
    for i := 0; i < len(cycle); i++ {
        from := cycle[i]
        to := cycle[(i+1)%len(cycle)]
        
        // Find edge amount
        for _, edge := range g.Edges[from] {
            if edge.To == to && edge.Token == token {
                if edge.Amount < minAmount {
                    minAmount = edge.Amount
                }
                break
            }
        }
    }

    return minAmount
}

func (g *Graph) ApplyNetting(cycle []string, token string, amount uint64) {
    // Subtract netting amount from each edge in cycle
    for i := 0; i < len(cycle); i++ {
        from := cycle[i]
        to := cycle[(i+1)%len(cycle)]
        
        // Find and update edge
        for j, edge := range g.Edges[from] {
            if edge.To == to && edge.Token == token {
                g.Edges[from][j].Amount -= amount
                break
            }
        }
    }
}

func (g *Graph) ToIntents() []Intent {
    intents := make([]Intent, 0)
    
    for from, edges := range g.Edges {
        for _, edge := range edges {
            if edge.Amount > 0 {
                intents = append(intents, Intent{
                    Sender:    from,
                    Receiver:  edge.To,
                    Token:     edge.Token,
                    Amount:    edge.Amount,
                })
            }
        }
    }
    
    return intents
}

func ProcessNetting(intents []Intent) []Intent {
    // Build graph
    g := NewGraph()
    for _, intent := range intents {
        g.AddEdge(intent.Sender, intent.Receiver, intent.Token, intent.Amount)
    }

    // Find SCCs
    sccs := g.FindSCCs()

    // Process each SCC
    for _, scc := range sccs {
        // Find cycles
        cycles := g.FindCycles(scc, 4) // Limit to cycles of length 4

        // Process each cycle
        for _, cycle := range cycles {
            // Get unique tokens in cycle
            tokenMap := make(map[string]bool)
            for i := 0; i < len(cycle); i++ {
                from := cycle[i]
                to := cycle[(i+1)%len(cycle)]
                for _, edge := range g.Edges[from] {
                    if edge.To == to {
                        tokenMap[edge.Token] = true
                    }
                }
            }

            // Process each token
            for token := range tokenMap {
                amount := g.CalculateNetting(cycle, token)
                if amount > 0 {
                    g.ApplyNetting(cycle, token, amount)
                }
            }
        }
    }

    // Convert back to intents
    return g.ToIntents()
}

func main() {
    // Example intents
    intents := []Intent{
        {Sender: "A", Receiver: "B", Token: "ETH", Amount: 100},
        {Sender: "B", Receiver: "C", Token: "ETH", Amount: 50},
        {Sender: "C", Receiver: "A", Token: "ETH", Amount: 30},
        {Sender: "D", Receiver: "E", Token: "USDC", Amount: 200},
        {Sender: "E", Receiver: "D", Token: "USDC", Amount: 200},
    }

    fmt.Println("Original intents:")
    for _, intent := range intents {
        fmt.Printf("%s -> %s: %d %s\n", 
            intent.Sender, intent.Receiver, intent.Amount, intent.Token)
    }

    // Process netting
    remainingIntents := ProcessNetting(intents)

    fmt.Println("\nRemaining intents after netting:")
    for _, intent := range remainingIntents {
        fmt.Printf("%s -> %s: %d %s\n", 
            intent.Sender, intent.Receiver, intent.Amount, intent.Token)
    }
}