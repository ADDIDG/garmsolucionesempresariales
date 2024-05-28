import asyncio
import concurrent.futures
import pandas as pd
import numpy as np
import aiohttp
import time
from typing import List

# Función para simular una carga de trabajo computacional
def heavy_computation(data: pd.DataFrame) -> pd.DataFrame:
    # Simulamos cálculos intensivos
    data['result'] = np.log(data['value'] + 1) * np.sqrt(data['value'])
    return data

# Función para generar un DataFrame grande con datos simulados
def generate_data(size: int) -> pd.DataFrame:
    np.random.seed(42)
    data = pd.DataFrame({
        'id': np.arange(size),
        'value': np.random.rand(size) * 1000
    })
    return data

# Clase que representa un nodo de procesamiento
class ProcessingNode:
    def __init__(self, node_id: int):
        self.node_id = node_id

    async def process_data(self, data: pd.DataFrame) -> pd.DataFrame:
        loop = asyncio.get_running_loop()
        with concurrent.futures.ThreadPoolExecutor() as pool:
            result = await loop.run_in_executor(pool, heavy_computation, data)
        return result

# Función para simular una petición HTTP asincrónica
async def fetch_data(session: aiohttp.ClientSession, url: str) -> pd.DataFrame:
    async with session.get(url) as response:
        json_response = await response.json()
        return pd.DataFrame(json_response)

# Función principal que coordina el procesamiento distribuido
async def main():
    data_size = 100000
    chunk_size = 10000
    nodes = [ProcessingNode(node_id=i) for i in range(4)]

    # Generamos los datos y los dividimos en partes para distribuir
    data = generate_data(data_size)
    chunks = [data[i:i + chunk_size] for i in range(0, data_size, chunk_size)]

    # Simulamos la obtención de datos adicionales mediante peticiones HTTP asincrónicas
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_data(session, f'https://jsonplaceholder.typicode.com/posts/{i}') for i in range(1, 5)]
        additional_data = await asyncio.gather(*tasks)

    # Procesamos los datos en paralelo usando los nodos de procesamiento
    tasks = []
    for i, chunk in enumerate(chunks):
        node = nodes[i % len(nodes)]
        tasks.append(node.process_data(chunk))

    results = await asyncio.gather(*tasks)
    final_result = pd.concat(results)

    print(final_result)

if __name__ == '__main__':
    start_time = time.time()
    asyncio.run(main())
    end_time = time.time()
    print(f"Processing completed in {end_time - start_time} seconds")
