/*
 * Small C program to generate BINARY2-encoded results. Pipe through base64 -w
 * 64 to create (hopefully) the same output as the BINARY2-encoder being
 * tested. Must be updated and rerun whenever the data in data.json changes.
 */

#include <arpa/inet.h>
#include <endian.h>
#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>


int
main(void)
{
    uint16_t bits;
    uint32_t i;
    int32_t j;
    uint32_t zero = htonl(0);
    uint64_t k;
    char c;
    int n;
    char s[10];
    float f;
    double g;
    long len;

    /* Null bitmap for first line. */
    bits = htons(0x5500);
    fwrite(&bits, sizeof(bits), 1, stdout);

    /* Data for first line. */
    i = htonl(1);
    fwrite(&i, sizeof(i), 1, stdout);       /* id */
    c = 0;
    fwrite(&c, sizeof(c), 1, stdout);       /* a */
    fwrite("c", sizeof(char), 1, stdout);   /* b */
    memset(&s, 0, sizeof(s));
    fwrite(&s, sizeof(s), 1, stdout);       /* c */
    len = strlen("https://example.com/datalink/1");
    i = htonl(len);
    fwrite(&i, sizeof(i), 1, stdout);       /* d length */
    fwrite("https://example.com/datalink/1", 1, len, stdout); /* d */
    g = NAN;
    memcpy(&k, &g, sizeof(k));
    k = htobe64(k);
    fwrite(&k, sizeof(k), 1, stdout);       /* e */
    f = -123.12;
    memcpy(&i, &f, sizeof(i));
    i = htonl(i);
    fwrite(&i, sizeof(i), 1, stdout);       /* f */
    i = 0;
    fwrite(&i, sizeof(i), 1, stdout);       /* g */
    k = htobe64(5294967296ULL);
    fwrite(&k, sizeof(k), 1, stdout);       /* k */

    /* Null bitmap for second line. */
    bits = htons(0x2A80);
    fwrite(&bits, sizeof(bits), 1, stdout);

    /* Data for second line. */
    i = htonl(2);
    fwrite(&i, sizeof(i), 1, stdout);       /* id */
    c = 1;
    fwrite(&c, sizeof(c), 1, stdout);       /* a */
    fwrite("\0", sizeof(char), 1, stdout);  /* b */
    memcpy(&s, "abcdefghi", sizeof(s));
    fwrite(&s, sizeof(s), 1, stdout);       /* c */
    fwrite(&zero, sizeof(zero), 1, stdout); /* d length */
    g = 1.13173E+98;
    memcpy(&k, &g, sizeof(k));
    k = htobe64(k);
    fwrite(&k, sizeof(k), 1, stdout);       /* e */
    f = NAN;
    memcpy(&i, &f, sizeof(i));
    i = htonl(i);
    fwrite(&i, sizeof(i), 1, stdout);       /* f */
    i = htonl(47);
    fwrite(&i, sizeof(i), 1, stdout);       /* g */
    k = 0;
    fwrite(&k, sizeof(k), 1, stdout);       /* k */

    exit(0);
}
