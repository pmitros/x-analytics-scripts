'''
A simple statistical significance test calculator which works well
for both small data and large. For large data, it does +-1.96 std.
div. of the sample. For small data, it integrates out the answer based
on a uniform prior.

Based on the blog article: On the amazing power of small data

'''
import doctest
import numpy as np

def _odds(phi, n, k):
    '''
    The odds of seeing k samples out of n, given a set of coin flips
    of bias phi.
    '''
    return (phi**k)*((1-phi)**(n-k))


def _total_odds(n, k, points=1000):
    '''
    Return the integral of the odds over phi from 0 to 1
    '''
    # A closed-form intergral requires hypergeometric functions,
    # so we approximate this by taking 10,000 points between 0
    # and 1. The end points are important, so we add them as well.
    # There is no change in results going from n=1000 to n=10,000
    # so we have good confidence in the answer
    phi = np.array(list(np.arange(0,1,1./points))+[1])  # 0, 0.1, ... 1.0
    unnormalized = odds(phi, n, k)  # Calculate odds at each point
    total_odds = sum(unnormalized)  # Integrate the odds so we can normalize
    normalized_p = unnormalized/total_odds  # Normalized odds
    maximum_likelyhood = np.argmax(normalized_p)  # Peak of the distribution
    # Now, we work out from the peak, adding maximum likelyhood points
    # until we hit our desired p value
    total_likelyhood = 0
    min_i = maximum_likelyhood
    max_i = maximum_likelyhood
    while total_likelyhood < 0.95:
        peak_i = np.argmax(normalized_p)
        total_likelyhood += normalized_p[peak_i]
        normalized_p[peak_i] = 0
        max_i = max(max_i, peak_i)
        min_i = min(min_i, peak_i)
        #print peak_i, total_likelyhood, normalized_p, max_i, min_i

    return (phi[min_i], phi[maximum_likelyhood], phi[max_i])
    #return "({min:.2},{max:.2})".format(min=phi[min_i], max=phi[max_i])


def _naive_odds(n,k):
    '''
    A simplified significance calculation, assumping sample variance
    is the same as population variance, and everything is normal. This
    starts to give the same results as `total_odds` for larger data
    sizes (where, coincidentally, `total_odds` begins to have floating
    point errors).
    '''
    mean = float(k)/float(n)
    bound = 1.96*np.sqrt(mean*(1-mean)/n)
    return (mean-bound, mean, mean+bound)
    #return "({min:.2},{max:.2})".format(min=mean-bound, max=mean+bound)


def survey_significance_bounds(n, k, cutoff=40):
    '''
    Calculate error bounds of a binary survey, RCT, or similar. If we
    have k positive responses to n questions, this will return the
    mean with error bounds as a tuple.

    n is the number of samples
    k is the number of successes
    cutoff is the place where we switch from integral with uniform
    prior to 1.96 std. div.
    '''
    if k<cutoff:
        return _total_odds(n,k)
    else:
        return _naive_odds(n,k)
