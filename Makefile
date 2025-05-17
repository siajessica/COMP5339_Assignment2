.PHONY: broker publisher subcriber clean

broker:
	mosquitto -v

publisher:
	python simple_retrieve_publish.py

app:
	streamlit run app.py

clean:
	rm -f *.csv