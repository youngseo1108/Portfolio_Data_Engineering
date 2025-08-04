import numpy as np

def gen_intervals(n, w, p, c):
  """
  Generates a list of intervals based on the given parameters.

  Parameters:
  n: The number of intervals to generate.
  w: The width of each interval.
  p: The overlap factor, a float between 0 and 1.
  c: Cases c=0 no overlap, c=1 all overlap c=2 some overlap

  Returns:
  A list of intervals, where each interval is a tuple of (start, end).
  """
  intervals = []
  T = []
  if c == 0:
    a = 0  # Start of first interval
    for _ in range(n):
      intervals.append((a,a + w))  # Interval [a_j, a_j + w]
      a += w+p  # Update starting point for next interval
  elif c == 1:
    a = 0  # Start of first interval
    for _ in range(n):
      intervals.append((a,a + w))  # Interval [a_j, a_j + w]
  else:
    a = 0  # Start of first interval
    for _ in range(n):
      intervals.append((a,a + w))  # Interval [a_j, a_j + w]
      a += w*(1-p)  # Update starting point for next interval
  np.random.shuffle(intervals)
  return intervals