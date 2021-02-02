DB is an embeddable transactional key-value store written in Go. Right now, it's
extremely barebones, everything is literally in memory. However, over time, I'd
love to keep hacking on this and add an on-disk persistance mechanism and a way
to make this thing distributed. Also I wrote a few tests to make sure this thing
kind of works.