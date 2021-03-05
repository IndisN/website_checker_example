## website_checker_example
### Overview
This is an example of a system that makes requests to URLs from list and stores results in database.
System is splitted into producer and consumer, that run separately. 
- Producer collects data using `aiohttp` and sends it to `Kafka`.
It optionally checks page contents for a provided regexp pattern and adds matched text to the result message.
- Consumer reads messages from `Kafka` and stores data into `PostgreSQL` overwriting data for existing URLs.
### Usage
1. Store config files for Kafka and PostgreSQL in `~/.kafka` and `~/.postgres`:
    ```shell script
    $ ls ~/.kafka
    ca.pem  service.cert  service.key
    $ ls ~/.postgres
    ca.pem  password.txt
    ```
2. Run `python3 setup.py bdist_wheel`. 
It will build `.whl` binaries for producer and consumer in `dist/`. 
Running with `--build_package [wsc-consumer, wsc-producer]` builds only specified `.whl`.
3. Install packages with `pip3 install dist/<package>.whl`.
4. Add `~/.local/bin` to `PATH` and run in separate terminals:
    ```shell script
   $ wsc_consumer 
   ```
   ```shell script
   $ wsc_producer -i input.txt
   ```
   where `input.txt` is a file without header, 
   containing lines of tab-separated urls 
   and optional corresponding regexp patterns.
### Testing
```python3 -m unittest discover test```