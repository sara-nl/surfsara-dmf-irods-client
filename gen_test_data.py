import os

SIZES = {'1k_': {'s': 1024,
                 'n': 5000},
         '1M_': {'s': 1024 * 1024,
                 'n': 500},
         '1G_': {'s': 1024 * 1024 * 1024,
                 'n': 50}}

p = os.path.join(os.path.dirname(__file__),
                 'test_data')

for k, d in SIZES.items():
    for idx in range(1, d['n'] + 1):
        fname = os.path.join(p, ("%s%04d.dat" % (k, idx)))
        size = d['s']
        with open(fname, 'wb') as fout:
            fout.write(os.urandom(size))
