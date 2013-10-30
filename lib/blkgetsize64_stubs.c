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
#include <sys/ioctl.h>

#include <fcntl.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>

#include <caml/alloc.h>
#include <caml/memory.h>
#include <caml/signals.h>
#include <caml/fail.h>
#include <caml/callback.h>
#include <caml/bigarray.h>

#include <linux/fs.h>

/* ocaml/ocaml/unixsupport.c */
extern void uerror(char *cmdname, value cmdarg);
#define Nothing ((value) 0)

CAMLprim value stub_blkgetsize64(value filename){
  CAMLparam1(filename);
  CAMLlocal1(result);
  uint64_t size_in_bytes;
  int fd;
  int success = -1;

  const char *filename_c = strdup(String_val(filename));

  enter_blocking_section();
  fd = open(filename_c, O_RDONLY, 0);
  if (ioctl(fd, BLKGETSIZE64, &size_in_bytes) == 0)
    success = 0;
  close(fd);
  leave_blocking_section();

  free((void*)filename_c);

  if (fd == -1) uerror("open", filename);
  if (success == -1) uerror("BLKGETSIZE64", filename);

  result = caml_copy_int64(size_in_bytes);
  CAMLreturn(result);
}
