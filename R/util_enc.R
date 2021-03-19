#' Saves (serializes) an encrypted object to disk
#' @param x The object to serialize
#' @param file The file name/path (must end in '.qs.enc')
#' @param nthreads Number of threads to use. Default 1.
#' @param public_key_path Path to public key
#' @export
qsenc_save <- function(x, file, nthreads = 1, public_key_path = Sys.getenv("ENCRYPTR_ID_RSA_PUB")){
  if(!stringr::str_detect(file, ".qs.enc$")){
    stop("file must end with '.qs.enc'")
  }
  tmp <- tempfile()
  on.exit(unlink(tmp))
  qs::qsave(
    x = x,
    file = tmp,
    nthreads = nthreads
  )

  openssl::encrypt_envelope(tmp, public_key_path) %>%
    saveRDS(file = file, compress = FALSE)
}

#' Reads an object in an encrypted file serialized to disk
#' @param file The file name/path (must end in '.qs.enc')
#' @param private_key_path Path to private key
#' @export
qsenc_read <- function(file, private_key_path = Sys.getenv("ENCRYPTR_ID_RSA")){
  if(!stringr::str_detect(file, ".qs.enc$")){
    stop("file must end with '.qs.enc'")
  }
  tmp <- tempfile()
  on.exit(unlink(tmp))

  .crypt = readRDS(file)
  zz = file(tmp, "wb")
  openssl::decrypt_envelope(
    .crypt$data,
    .crypt$iv,
    .crypt$session,
    key = private_key_path,
    password = NULL
  ) %>%
    writeBin(zz)
  close(zz)

  a <- qs::qread(tmp)
  a
}
