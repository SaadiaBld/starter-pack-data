FROM spark:3.5.7-python3

USER root

RUN pip install --no-cache-dir jupyterlab pyspark==3.5.7

WORKDIR /workspace

COPY requirements.txt /workspace/
RUN pip install --no-cache-dir -r requirements.txt

COPY . /workspace

EXPOSE 8080
EXPOSE 8888

#commandes pour lancer jupyter notebook
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]

