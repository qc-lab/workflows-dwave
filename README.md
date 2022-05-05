# workflows-dwave

To run the app with classic solver you should use the following commmand template

```main.py -i <input_workflow> -o <output_workflow> -m <target_machines_file> -d <deadline_in_ms>```

If you want to use qunatum hybrid solver instead you need to first export your dwave api token.

``` export DWAVE_API_TOKEN=XXX-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx```

then you can run 

```main.py -q -i <input_workflow> -o <output_workflow> -m <target_machines_file> -d <deadline_in_ms>```
