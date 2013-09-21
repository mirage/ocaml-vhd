/*
 * Copyright (C) 2012-2013 Citrix Inc
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published
 * by the Free Software Foundation; version 2.1 only. with the special
 * exception on linking described in file LICENSE.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 */

#define _GNU_SOURCE

#include <sys/types.h>
#include <sys/stat.h>

#include <fcntl.h>
#include <string.h>
#include <unistd.h>

#include <caml/alloc.h>
#include <caml/memory.h>
#include <caml/signals.h>
#include <caml/fail.h>
#include <caml/callback.h>
#include <caml/bigarray.h>

/* ocaml/ocaml/unixsupport.c */
extern void uerror(char *cmdname, value cmdarg);
#define Nothing ((value) 0)

CAMLprim value stub_openfile_direct(value filename, value mode){
  CAMLparam2(filename, mode);
  CAMLlocal1(result);
  int fd;

  const char *filename_c = strdup(String_val(filename));

  enter_blocking_section();
  fd = open(filename_c, O_RDWR | O_DIRECT, Int_val(mode));
  leave_blocking_section();

  free((void*)filename_c);

  if (fd == -1) uerror("open", filename);

  CAMLreturn(Val_int(fd));
}

CAMLprim value stub_fsync (value fd)
{
  CAMLparam1(fd);
  int c_fd = Int_val(fd);
  if (fsync(c_fd) != 0) uerror("fsync", Nothing);
  CAMLreturn(Val_unit);
}

/* From mirage:
   https://github.com/mirage/mirage/commit/3d7aace5eb162f99b79b21d1784f87ce26fec8b7
*/

#define PAGE_SIZE 4096
#include <stdlib.h>
#include <malloc.h>

/* Allocate a page-aligned bigarray of length [n_pages] pages.
   Since CAML_BA_MANAGED is set the bigarray C finaliser will
   call free() whenever all sub-bigarrays are unreachable.
 */
CAMLprim value
caml_alloc_pages(value n_pages)
{
  CAMLparam1(n_pages);
  size_t len = Int_val(n_pages) * PAGE_SIZE;
  /* If the allocation fails, return None. The ocaml layer will
     be able to trigger a full GC which just might run finalizers
     of unused bigarrays which will free some memory. */
  void* block = memalign(PAGE_SIZE, len);

  if (block == NULL) {
    caml_failwith("memalign");
  }
  CAMLreturn(caml_ba_alloc_dims(CAML_BA_UINT8 | CAML_BA_C_LAYOUT | CAML_BA_MANAGED, 1, block, len));
}

