"""Graph database client and analytics (ArangoDB-inspired)."""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List, Dict, Any, Set, Tuple
import sqlite3
import json
from pathlib import Path
import sys
from collections import deque

@dataclass
class Vertex:
    id: str
    collection: str
    properties: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)

@dataclass
class Edge:
    id: str
    from_id: str
    to_id: str
    collection: str
    label: str
    weight: float = 1.0
    properties: Dict[str, Any] = field(default_factory=dict)

class GraphDB:
    def __init__(self, db_path: str = None):
        if db_path is None:
            db_path = Path.home() / ".blackroad" / "graphdb.db"
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
    
    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS vertices (
            id TEXT PRIMARY KEY, collection TEXT NOT NULL, properties TEXT, created_at REAL)''')
        c.execute('''CREATE TABLE IF NOT EXISTS edges (
            id TEXT PRIMARY KEY, from_id TEXT, to_id TEXT, collection TEXT,
            label TEXT, weight REAL, properties TEXT)''')
        c.execute('''CREATE TABLE IF NOT EXISTS graphs (name TEXT PRIMARY KEY, created_at REAL)''')
        conn.commit()
        conn.close()
    
    def create_graph(self, name: str) -> Dict:
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('INSERT OR IGNORE INTO graphs VALUES (?, ?)',
                  (name, datetime.utcnow().timestamp()))
        conn.commit()
        conn.close()
        return {"name": name, "created_at": datetime.utcnow().isoformat()}
    
    def get_graph(self, name: str) -> Optional[Dict]:
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('SELECT * FROM graphs WHERE name = ?', (name,))
        row = c.fetchone()
        conn.close()
        return {"name": row[0], "created_at": datetime.fromtimestamp(row[1]).isoformat()} if row else None
    
    def insert_vertex(self, graph: str, collection: str, properties: Dict) -> str:
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('SELECT COUNT(*) FROM vertices WHERE collection = ?', (collection,))
        count = c.fetchone()[0]
        vertex_id = f"{collection}/{count}"
        c.execute('INSERT INTO vertices VALUES (?, ?, ?, ?)',
                  (vertex_id, collection, json.dumps(properties), datetime.utcnow().timestamp()))
        conn.commit()
        conn.close()
        return vertex_id
    
    def insert_edge(self, graph: str, from_id: str, to_id: str, label: str,
                    weight: float = 1.0, properties: Dict = None) -> str:
        properties = properties or {}
        edge_id = f"{from_id}->{to_id}"
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('INSERT INTO edges VALUES (?, ?, ?, ?, ?, ?, ?)',
                  (edge_id, from_id, to_id, graph, label, weight, json.dumps(properties)))
        conn.commit()
        conn.close()
        return edge_id
    
    def get_neighbors(self, vertex_id: str, direction: str = "both", label: Optional[str] = None) -> List[Vertex]:
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        neighbors = []
        
        if direction in ("both", "out"):
            query = 'SELECT to_id FROM edges WHERE from_id = ?'
            params = [vertex_id]
            if label:
                query += ' AND label = ?'
                params.append(label)
            c.execute(query, params)
            for (nid,) in c.fetchall():
                c.execute('SELECT * FROM vertices WHERE id = ?', (nid,))
                v = c.fetchone()
                if v:
                    neighbors.append(Vertex(id=v[0], collection=v[1], properties=json.loads(v[2])))
        
        if direction in ("both", "in"):
            query = 'SELECT from_id FROM edges WHERE to_id = ?'
            params = [vertex_id]
            if label:
                query += ' AND label = ?'
                params.append(label)
            c.execute(query, params)
            for (nid,) in c.fetchall():
                c.execute('SELECT * FROM vertices WHERE id = ?', (nid,))
                v = c.fetchone()
                if v:
                    neighbors.append(Vertex(id=v[0], collection=v[1], properties=json.loads(v[2])))
        conn.close()
        return neighbors
    
    def shortest_path(self, graph: str, from_id: str, to_id: str) -> Optional[List[str]]:
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        queue = deque([(from_id, [from_id])])
        visited = {from_id}
        
        while queue:
            current, path = queue.popleft()
            if current == to_id:
                conn.close()
                return path
            c.execute('SELECT to_id FROM edges WHERE from_id = ?', (current,))
            for (neighbor,) in c.fetchall():
                if neighbor not in visited:
                    visited.add(neighbor)
                    queue.append((neighbor, path + [neighbor]))
        conn.close()
        return None
    
    def traverse(self, graph: str, start_id: str, depth: int = 3) -> Dict:
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        visited, vertices, edges = set(), {}, []
        
        def dfs(v_id, d):
            if d > depth or v_id in visited:
                return
            visited.add(v_id)
            c.execute('SELECT * FROM vertices WHERE id = ?', (v_id,))
            v = c.fetchone()
            if v:
                vertices[v[0]] = {"id": v[0], "collection": v[1], "properties": json.loads(v[2])}
            c.execute('SELECT * FROM edges WHERE from_id = ?', (v_id,))
            for e in c.fetchall():
                edges.append({"from": e[1], "to": e[2], "label": e[4]})
                dfs(e[2], d + 1)
        
        dfs(start_id, 0)
        conn.close()
        return {"vertices": vertices, "edges": edges}
    
    def aql_query(self, query_str: str) -> List[Dict]:
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        parts = query_str.split()
        if "FOR" not in parts or "IN" not in parts or "RETURN" not in parts:
            conn.close()
            return []
        collection = parts[parts.index("IN") + 1]
        c.execute('SELECT * FROM vertices WHERE collection = ?', (collection,))
        return [{"id": r[0], "collection": r[1], "properties": json.loads(r[2])} for r in c.fetchall()]
    
    def get_degree_centrality(self, graph: str) -> List[Tuple[str, int]]:
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('SELECT id FROM vertices')
        vertices = [r[0] for r in c.fetchall()]
        degrees = {}
        for v_id in vertices:
            c.execute('SELECT COUNT(*) FROM edges WHERE from_id = ? OR to_id = ?', (v_id, v_id))
            degrees[v_id] = c.fetchone()[0]
        conn.close()
        return sorted(degrees.items(), key=lambda x: x[1], reverse=True)
    
    def find_communities(self, graph: str) -> List[Set[str]]:
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('SELECT id FROM vertices')
        all_vertices = {r[0] for r in c.fetchall()}
        communities, visited = [], set()
        
        def dfs_comp(start):
            stack = [start]
            comp = set()
            while stack:
                v = stack.pop()
                if v in visited:
                    continue
                visited.add(v)
                comp.add(v)
                c.execute('SELECT to_id FROM edges WHERE from_id = ?', (v,))
                for (n,) in c.fetchall():
                    if n not in visited:
                        stack.append(n)
            return comp
        
        for v in all_vertices:
            if v not in visited:
                communities.append(dfs_comp(v))
        conn.close()
        return communities
    
    def export_gephi(self, graph: str, output_path: str) -> bool:
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        Path(output_path).mkdir(parents=True, exist_ok=True)
        c.execute('SELECT id, collection FROM vertices')
        with open(f"{output_path}/nodes.csv", "w") as f:
            f.write("Id,Label,Collection\n")
            for row in c.fetchall():
                f.write(f'{row[0]},{row[0]},{row[1]}\n')
        c.execute('SELECT from_id, to_id, label, weight FROM edges')
        with open(f"{output_path}/edges.csv", "w") as f:
            f.write("Source,Target,Label,Weight\n")
            for row in c.fetchall():
                f.write(f'{row[0]},{row[1]},{row[2]},{row[3]}\n')
        conn.close()
        return True

def main():
    db = GraphDB()
    if len(sys.argv) < 2:
        print("Usage: python graph_db.py [insert-vertex|path|centrality]")
        return
    cmd = sys.argv[1]
    if cmd == "insert-vertex" and len(sys.argv) >= 4:
        collection, props = sys.argv[2], json.loads(sys.argv[3]) if sys.argv[3].startswith('{') else {"value": sys.argv[3]}
        vertex_id = db.insert_vertex("default", collection, props)
        print(f"Created: {vertex_id}")
    elif cmd == "path" and len(sys.argv) >= 5:
        graph, from_id, to_id = sys.argv[2], sys.argv[3], sys.argv[4]
        path = db.shortest_path(graph, from_id, to_id)
        print(f"Path: {' -> '.join(path) if path else 'No path'}")
    elif cmd == "centrality" and len(sys.argv) >= 3:
        graph = sys.argv[2]
        for vid, degree in db.get_degree_centrality(graph)[:10]:
            print(f"  {vid}: {degree}")

if __name__ == "__main__":
    main()
