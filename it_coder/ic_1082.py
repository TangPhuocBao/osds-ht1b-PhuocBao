class Node:
    def __init__(self, parent = None):
        self.count = 1
        self.prev = parent

class Tree:
    def __init__(self):
        self.node_map = None
    
    def append(self, parent, child):
        if self.node_map is None:
            self.node_map = {}
            
            node = Node()
            node.count += 1
            self.node_map[parent] = node
            
            node = Node(parent)
            self.node_map[child] = node
            return
        self._append(parent, child)
        
    def _append(self, parent, child):
        node = Node(parent)
        self.node_map[child] = node
        
        cur_node = self.node_map[parent]
        cur_node.count += 1
        while cur_node.prev:
            cur_node = self.node_map[cur_node.prev]
            cur_node.count += 1
        
    def print_result(self, order):
        arr = [self.node_map[key].count for key in order]
        print(*arr)
        
def main():
    t = Tree()
    for _ in range(int(input()) - 1):
        p, c = input().split()
        t.append(p, c)
    order = input().split()
    t.print_result(order)

main()