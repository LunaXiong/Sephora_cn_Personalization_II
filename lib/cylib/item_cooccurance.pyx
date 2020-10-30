import numpy as np
cimport numpy as np
cimport cython


cdef float __pred_prob(
        np.ndarray[np.float32_t, ndim=1] i_vec,
        np.ndarray[np.int8_t, ndim=1] owc,
        np.ndarray[np.float32_t, ndim=2] owl,
        int cnt
):
    cdef np.float64_t log_cnt
    cdef np.float32_t ret
    cdef np.ndarray[np.float32_t, ndim=1] dot
    cdef np.ndarray[np.float32_t, ndim=1] prob
    cdef int zero
    zero = 0

    dot = np.dot(i_vec, owl)
    prob = np.logaddexp(zero, -dot) + owc*dot
    ret = -np.sum(prob)
    return ret


def pred_prob(i_vec, ows, owl, cnt):
    return __pred_prob(i_vec, ows, owl, cnt)
