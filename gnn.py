import pandas as pd
import numpy as np
import torch
import torch.nn.functional as F
from torch_geometric.data import Data
from torch_geometric.nn import GCNConv # Graph Convolutional Network

# 1. CARGA Y FILTRADO
df = pd.read_csv('master_tourism_dataset.csv')
df['date'] = pd.to_datetime(df['date'])

# Vamos a enfocarnos en ALBANIA para demostrar el modelo
country_df = df[df['country'] == 'Albania'].sort_values(['date', 'city'])

# 2. PREPARAR NODOS Y ESTRUCTURA DEL GRAFO
cities = country_df['city'].unique()
city_to_idx = {city: i for i, city in enumerate(cities)}
num_cities = len(cities)

# Crear aristas (todos conectados con todos dentro del país - Fully Connected)
edge_index = []
for i in range(num_cities):
    for j in range(num_cities):
        if i != j:
            edge_index.append([i, j])
edge_index = torch.tensor(edge_index, dtype=torch.long).t().contiguous()

# 3. DEFINICIÓN DE LA RED (GNN)
class TourismGNN(torch.nn.Module):
    def __init__(self, num_features):
        super(TourismGNN, self).__init__()
        self.conv1 = GCNConv(num_features, 16)
        self.conv2 = GCNConv(16, 1) # Predice 1 valor: nights_spent_country

    def forward(self, x, edge_index):
        # x: Atributos de las ciudades (Google Trends, etc)
        x = self.conv1(x, edge_index)
        x = F.relu(x)
        x = self.conv2(x, edge_index)
        return x

# 4. FORMATEO DE DATOS PARA UN MES ESPECÍFICO (Ejemplo: Agosto 2023)
# En un modelo real, iteraríamos por todos los meses
target_date = '2023-08-01'
month_data = country_df[country_df['date'] == target_date]

# Features: gt_hotel, gt_airbnb, gt_flights
x_features = torch.tensor(month_data[['gt_hotel', 'gt_airbnb', 'gt_flights']].values, dtype=torch.float)
y_target = torch.tensor(month_data['nights_spent_country'].values, dtype=torch.float).view(-1, 1)

# Crear el objeto Data de PyG
graph_data = Data(x=x_features, edge_index=edge_index, y=y_target)

# 5. EJECUCIÓN DEL MODELO
model = TourismGNN(num_features=3)
prediction = model(graph_data.x, graph_data.edge_index)

print(f"Predicciones de carga turística por nodo para {target_date}:")
for i, city in enumerate(cities):
    print(f"Ciudad: {city} | Valor GNN: {prediction[i].item():.4f}")