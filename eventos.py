from mpi4py import MPI
def readFile(archivo):
    data = []
    with open(archivo, 'r') as file:
        for line in file:
            estacion, temperatura = line.strip().split(';')
            data.append((estacion, float(temperatura)))
    return data


def calcular(data):
    tempMin = min(data, key=lambda x: x[1])[1]
    tempMax = max(data, key=lambda x: x[1])[1]
    tempProm = sum(temp for _, temp in data) / len(data)
    return tempMin, tempMax, tempProm


def main():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    if rank == 0:
        data = readFile('entrada2.txt')
        chunk_size = len(data) // (size - 1)
        chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]

        for i in range(1, size):
            comm.send(chunks[i - 1], dest=i)

        valores = []
        for i in range(1, size):
            valores.append(comm.recv(source=i))

        estadisticasEstacion = {}
        for resultado in valores:
            for estacion, estadisticas in resultado.items():
                if estacion in estadisticasEstacion:
                    estadisticasEstacion[estacion].append(estadisticas)
                else:
                    estadisticasEstacion[estacion] = [estadisticas]

        for estacion, estadisticas_list in estadisticasEstacion.items():
            tempMin, tempMax, tempProm = zip(*estadisticas_list)
            tempMin = min(tempMin)
            tempMax = max(tempMax)
            tempProm = sum(tempProm) / len(tempProm)
            print(f"Estacion: {estacion}, Temp Min: {tempMin}, Temp Max: {tempMax}, Temp Prom: {tempProm}")

    else:
        data_chunk = comm.recv(source=0)
        estadisticasEstacion = {}
        for estacion, temperatura in data_chunk:
            if estacion in estadisticasEstacion:
                estadisticasEstacion[estacion].append(temperatura)
            else:
                estadisticasEstacion[estacion] = [temperatura]

        resultado = {}
        for estacion, temps in estadisticasEstacion.items():
            estadisticas = calcular([(estacion, temp) for temp in temps])
            resultado[estacion] = estadisticas

        comm.send(resultado, dest=0)


if __name__ == "__main__":
    main()

#para probar: mpiexec -n 2 python eventos.py