import sqlite3
from datetime import datetime

# Conectar ao banco de dados
conn = sqlite3.connect('/home/miguelo/Documents/pipeline-airflow/dags/etl_data.db')
cursor = conn.cursor()

# Criar tabela de usuários
cursor.execute('''
CREATE TABLE IF NOT EXISTS usuarios (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nome TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    telefone TEXT,
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ativo BOOLEAN DEFAULT 1
)
''')

# Inserir dados de exemplo
usuarios = [
    ('João Silva', 'joao.silva@email.com', '11987654321'),
    ('Maria Santos', 'maria.santos@email.com', '11987654322'),
    ('Pedro Oliveira', 'pedro.oliveira@email.com', '11987654323'),
    ('Ana Costa', 'ana.costa@email.com', '11987654324'),
    ('Carlos Ferreira', 'carlos.ferreira@email.com', '11987654325'),
    ('Juliana Lima', 'juliana.lima@email.com', '11987654326'),
    ('Roberto Alves', 'roberto.alves@email.com', '11987654327'),
    ('Beatriz Rocha', 'beatriz.rocha@email.com', '11987654328'),
]

cursor.executemany(
    'INSERT INTO usuarios (nome, email, telefone) VALUES (?, ?, ?)',
    usuarios
)

# Confirmar as mudanças
conn.commit()

# Verificar os dados inseridos
cursor.execute('SELECT * FROM usuarios')
rows = cursor.fetchall()

print(f"Banco de dados criado com sucesso!")
print(f"Total de usuários inseridos: {len(rows)}")
print("\nDados inseridos:")
for row in rows:
    print(f"  ID: {row[0]}, Nome: {row[1]}, Email: {row[2]}, Telefone: {row[3]}")

# Fechar a conexão
conn.close()
