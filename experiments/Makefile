.PHONY: experiments plot

plot:
	./plot_word_count.sh

experiments:
	reset
	./run_scaling.py
	./plot_word_count.sh

clean:
	rm -rf results plots
