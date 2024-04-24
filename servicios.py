# 2) El servicio ReadFileService se encarga de leer el archivo y luego el servicio CalcularTemperaturaService se 
# encarga de obtener la temp. minima, la maxima y el promedio. Posteriormente a esto imprime las estaciones 
# y las temperaturas respectivas.

# Servicio 1 : ReadFileService
class ReadFileService:
    def __init__(self, archivo):
        self.archivo = archivo

    def readFile(self):
        data = []
        with open(self.archivo, 'r') as file:
            for linea in file:
                estacion, temperatura = linea.strip().split(';')
                data.append((estacion, float(temperatura)))
        return data


# Servicio 2: calcularTemperaturaService
class calcularTemperaturaService:
    def __init__(self, data):
        self.data = data

    def calcularTemperaturas(self):
        estacion_temperaturas = {}
        for estacion, temperatura in self.data:
            if estacion not in estacion_temperaturas:
                estacion_temperaturas[estacion] = [temperatura]
            else:
                estacion_temperaturas[estacion].append(temperatura)

        valores = {}
        for estacion, temperaturas in estacion_temperaturas.items():
            tempMin = min(temperaturas)
            tempMax = max(temperaturas)
            tempProm = sum(temperaturas) / len(temperaturas)
            valores[estacion] = {'min': tempMin, 'max': tempMax, 'avg': tempProm}

        return valores


### BLOQUE PRINCIPAL ###
if __name__ == "__main__":
    archivo = "entrada2.txt"

    # Llamada al servicio ReadFileService
    lecturaArchivo = ReadFileService(archivo)
    data = lecturaArchivo.readFile()

    # Llamada al servicio calcularTemperaturaService
    calcularTemps = calcularTemperaturaService(data)
    valores = calcularTemps.calcularTemperaturas()

    # Print valores
    for estacion, temps in valores.items():
        print(f"Estación: {estacion}, Temp Min: {temps['min']}, Temp Max: {temps['max']}, Temp Prom: {temps['avg']}")
