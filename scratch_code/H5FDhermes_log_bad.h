/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * Copyright by the Board of Trustees of the University of Illinois.         *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5.  The full HDF5 copyright notice, including     *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the root of the source code       *
 * distribution tree, or in https://www.hdfgroup.org/licenses.               *
 * If you do not have access to either file, you may request a copy from     *
 * help@hdfgroup.org.                                                        *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/*
 * Programmer:  Meng Tang
 *              Sep 2022
 *
 * Purpose: The hermes file driver using only the HDF5 public API
 *          and buffer datasets in Hermes buffering systems with
 *          multiple storage tiers.
 */
#ifndef _GNU_SOURCE
  #define _GNU_SOURCE
#endif

#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <math.h>

#include <unistd.h>
#include <sys/file.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <mpi.h>

/* HDF5 header for dynamic plugin loading */
#include "H5PLextern.h"

#include "H5FDhermes.h"     /* Hermes file driver     */
// #include "H5FDhermes_err.h" /* error handling         */

// /* Necessary hermes headers */
#include "hermes_wrapper.h"


#include <time.h>       // for struct timespec, clock_gettime(CLOCK_MONOTONIC, &end);
/* candice added functions for I/O traces end */

typedef struct Dset_access_t {
  char      dset_name[H5L_MAX_LINK_NAME_LEN];
  haddr_t   dset_offset;
  int       dset_ndim;
  hssize_t    dset_npoints;
  hsize_t   *dset_dim;
} Dset_access_t;

typedef struct File_access_t {
  char      filename[H5L_MAX_LINK_NAME_LEN];
  char      type[10]; /* open, read, write, close */
  size_t    file_size;
  size_t    access_size; /* data access size */
  size_t    page_start;
  size_t    page_end;
  size_t    page_num;
  haddr_t   addr_start;
  haddr_t   addr_end;
  char      *mem_type;
  size_t    ndsets;
  Dset_access_t * dsets;

} File_access_t;

/* candice added, print H5FD_mem_t H5FD_MEM_OHDR type more info */
void print_ohdr_type(H5F_mem_t type){

  if (type == H5FD_MEM_FHEAP_HDR){
    printf("- Access_Region_Mem_Type : H5FD_MEM_FHEAP_HDR \n");

  } else if( type == H5FD_MEM_FHEAP_IBLOCK ){
    printf("- Access_Region_Mem_Type : H5FD_MEM_FHEAP_IBLOCK \n");

  } else if( type == H5FD_MEM_FSPACE_HDR ){
    printf("- Access_Region_Mem_Type : H5FD_MEM_FSPACE_HDR \n");

  } else if( type == H5FD_MEM_SOHM_TABLE  ){
    printf("- Access_Region_Mem_Type : H5FD_MEM_SOHM_TABLE  \n");

  } else if( type == H5FD_MEM_EARRAY_HDR ){
    printf("- Access_Region_Mem_Type : H5FD_MEM_EARRAY_HDR \n");

  } else if( type == H5FD_MEM_EARRAY_IBLOCK ){
    printf("- Access_Region_Mem_Type : H5FD_MEM_EARRAY_IBLOCK \n");

  } else if( type == H5FD_MEM_FARRAY_HDR  ){
    printf("- Access_Region_Mem_Type : H5FD_MEM_FARRAY_HDR  \n");

  } else {
    printf("- Access_Region_Mem_Type : H5FD_MEM_OHDR \n");
  }
}

void print_mem_type(H5F_mem_t type){
  if (type == H5FD_MEM_DEFAULT){
    printf("- Access_Region_Mem_Type : H5FD_MEM_DEFAULT \n");

  } else if( type == H5FD_MEM_SUPER ){
    printf("- Access_Region_Mem_Type : H5FD_MEM_SUPER \n");

  } else if( type == H5FD_MEM_BTREE ){
    printf("- Access_Region_Mem_Type : H5FD_MEM_BTREE \n");

  } else if( type == H5FD_MEM_DRAW ){
    printf("- Access_Region_Mem_Type : H5FD_MEM_DRAW \n");

  } else if( type == H5FD_MEM_GHEAP ){
    printf("- Access_Region_Mem_Type : H5FD_MEM_GHEAP \n");

  } else if( type == H5FD_MEM_LHEAP ){
    printf("- Access_Region_Mem_Type : H5FD_MEM_LHEAP \n");

  } else if( type == H5FD_MEM_OHDR ){
    print_ohdr_type(type);

  } else if( type == H5FD_MEM_NTYPES ){
    printf("- Access_Region_Mem_Type : H5FD_MEM_NTYPES \n");

  } else if( type == H5FD_MEM_NOLIST ){
    printf("- Access_Region_Mem_Type : H5FD_MEM_NOLIST \n");

  } else {
    printf("- Access_Region_Mem_Type : NOT_VALID \n");
  }
}

/* candice added, print H5FD_mem_t H5FD_MEM_OHDR type more info */
void record_ohdr_type(H5F_mem_t type, File_access_t * access){

  if (type == H5FD_MEM_FHEAP_HDR){
    strcpy(access->mem_type, "H5FD_MEM_FHEAP_HDR");

  } else if( type == H5FD_MEM_FHEAP_IBLOCK ){
    strcpy(access->mem_type, "H5FD_MEM_FHEAP_IBLOCK");

  } else if( type == H5FD_MEM_FSPACE_HDR ){
    strcpy(access->mem_type, "H5FD_MEM_FSPACE_HDR");

  } else if( type == H5FD_MEM_SOHM_TABLE  ){
    strcpy(access->mem_type, "H5FD_MEM_SOHM_TABLE");

  } else if( type == H5FD_MEM_EARRAY_HDR ){
    strcpy(access->mem_type, "H5FD_MEM_EARRAY_HDR");

  } else if( type == H5FD_MEM_EARRAY_IBLOCK ){
    strcpy(access->mem_type, "H5FD_MEM_EARRAY_IBLOCK");

  } else if( type == H5FD_MEM_FARRAY_HDR  ){
    strcpy(access->mem_type, "H5FD_MEM_FARRAY_HDR");

  } else {
    strcpy(access->mem_type, "H5FD_MEM_OHDR");
  }
}


/* candice added, print H5FD_mem_t memory allocated type */
void record_mem_type(H5F_mem_t type, File_access_t * access){
  // https://docs.hdfgroup.org/archive/support/HDF5/doc/RM/RM_H5F.html

  access->mem_type = malloc(30*sizeof(char));

  if (type == H5FD_MEM_DEFAULT){
    strcpy(access->mem_type, "H5FD_MEM_SUPER");

  } else if( type == H5FD_MEM_SUPER ){
    strcpy(access->mem_type, "H5FD_MEM_SUPER");

  } else if( type == H5FD_MEM_BTREE ){
    strcpy(access->mem_type, "H5FD_MEM_BTREE");

  } else if( type == H5FD_MEM_DRAW ){
    strcpy(access->mem_type, "H5FD_MEM_DRAW");

  } else if( type == H5FD_MEM_GHEAP ){
    strcpy(access->mem_type, "H5FD_MEM_GHEAP");

  } else if( type == H5FD_MEM_LHEAP ){
    strcpy(access->mem_type, "H5FD_MEM_LHEAP");

  } else if( type == H5FD_MEM_OHDR ){
    record_ohdr_type(type, access);

  } else if( type == H5FD_MEM_NTYPES ){
    strcpy(access->mem_type, "H5FD_MEM_NTYPES");

  } else if( type == H5FD_MEM_NOLIST ){
    strcpy(access->mem_type, "H5FD_MEM_NOLIST");

  } else {
    strcpy(access->mem_type, "NOT_VALID");
  }
}


void init_file_access_struct(File_access_t * acc){

  acc->file_size = -1;
  acc->access_size = -1;
  acc->page_start = -1;
  acc->page_end = -1;
  acc->page_num = -1;
  acc->addr_start = -1;
  acc->addr_end = -1;
  acc->ndsets = -1;

}


void * print_data_access_record(File_access_t * acc){

  printf("Access_Type : %s\n", acc->type);
  printf("Filename : %s\n", acc->filename);
  printf("Filesize : %s\n", acc->file_size);
  printf("- Access_Size : %ld\n", acc->access_size);
  printf("- Access_Region_Mem_Type : %s\n", acc->mem_type);
  printf("- Start_Page : %ld\n", acc->page_start);
  printf("- End_Page : %ld\n", acc->page_end);
  printf("- Number_Pages : %ld\n", acc->page_num);
  printf("- Start_Address : %ld\n", acc->addr_start);
  printf("- End_Address : %ld\n", acc->addr_end);
  printf("- Num_Datasets : %ld\n", acc->ndsets);


  if( acc->ndsets != -1 ){
    

    for(int i=0; i< acc->ndsets; i++){
      printf("-- Dataset_Name : %s\n", acc->dsets[i].dset_name);
      printf("-- Dataset_Offset : %ld\n", acc->dsets[i].dset_offset);
      printf("-- Dataset_N_Points : %d\n", acc->dsets[i].dset_npoints);
      printf("-- Dataset_N_Dimension : %d\n", acc->dsets[i].dset_ndim);
      
      hsize_t dset_ndim = acc->dsets[i].dset_ndim;

      printf("-- Dataset_Dimension : {");
      for (int j=0; j < dset_ndim; j++){
        printf("%ld,", (int) acc->dsets[i].dset_dim[j]);
      }
      printf("}\n");
    }

  }

  printf("\n");

}

/* candice added, for printout dataset dimension info */
void * print_dim_info(hid_t space_id){

  // H5Sget_simple_extent_dims
  int d = H5Sget_simple_extent_ndims(space_id);
  printf("-- Dataset_N_Dimension : %ld\n", d);
  int np = H5Sget_simple_extent_npoints(space_id);
  printf("-- Dataset_N_Points : %ld\n", np);
  hsize_t 	dims[d];
  hsize_t 	maxdims[d];
  H5Sget_simple_extent_dims(space_id,dims,maxdims);
  printf("-- Dataset_Dimension : {");
  for (int i=0; i < d; i++){
    printf("%d,",dims[i]);
  }
  // printf("} maxdims {");
  // for (int i=0; i < d; i++){
  //   printf("%d,",maxdims[i]);
  // }
  printf("}\n");
  
}

/* candice added, for printout dataset info */
void print_dset_info(hid_t dset_id){

  char dset_name[50];

  if((H5Iget_name(dset_id,dset_name, 100*sizeof(char))) < 0){
    printf("-- Dataset_Name : Error\n");
  } else {
    printf("-- Dataset_Name : %s\n", dset_name); 
  }

  // H5Dget_offset
  // if(H5Dget_offset(dset_id) == HADDR_UNDEF){
  //   printf("-- Dataset_Offset : Error (HADDR_UNDEF)\n");
  // }else {
  //   printf("-- Dataset_Offset : %zu\n", H5Dget_offset(dset_id));
  // }

  printf("-- Dataset_Offset : %ld\n", H5Dget_offset(dset_id));

  hid_t space_id = H5Dget_space(dset_id);
  print_dim_info(space_id);

  // // H5Dget_num_chunks : error, Segmentation fault      (core dumped)
  // hsize_t   nchunks;
  // H5Dget_num_chunks(dset_id, H5S_ALL, &nchunks);
  // printf("H5Dget_num_chunks : %zu\n", nchunks);

  // H5Dget_chunk_info_by_coord
  // unsigned * 	filter_mask;
  // haddr_t * 	addr;
  // hsize_t * 	size;
  // haddr_t   offset = H5Dget_offset(dset_id);
  // H5Dget_chunk_info_by_coord(dset_id, &offset, filter_mask, addr,size);
  // printf("H5Dget_chunk_info_by_coord : filter_mask=%ld addr=%zu size=%zu\n", 
  //   filter_mask, addr, size);


  // H5Dget_access_plist
  // H5Dget_create_plist
  // hsize_t vlen_size;
  // H5Dvlen_get_buf_size(dset_id, H5Dget_type(dset_id), H5Dget_space(dset_id),&vlen_size);
  // printf("-- H5Dvlen_get_buf_size : %ld\n", vlen_size);

  // printf("\n");

}

void * print_all_dset(){
  size_t dset_count = H5Fget_obj_count(H5F_OBJ_ALL,H5F_OBJ_DATASET);

  hid_t dset_id_list[dset_count];
  hid_t dset_id;

  if( dset_count == 0){
    printf("- Num_Datasets : 0\n");
  } else {
    if((H5Fget_obj_ids(H5F_OBJ_ALL, H5F_OBJ_DATASET, dset_count, dset_id_list)) < 0){
      printf("- Num_Datasets : Error\n");
    } else {
      printf("- Num_Datasets : %ld\n",dset_count);
      for (int i=0; i< dset_count; i++){
        print_dset_info(dset_id_list[i]);
      }
    }
  }


  printf("\n");
}

/* candice added, record dataset info */
void record_all_dset(File_access_t * access){

  // H5F_OBJ_ALL   
  // (H5F_OBJ_FILE | H5F_OBJ_DATASET | H5F_OBJ_GROUP | H5F_OBJ_DATATYPE | H5F_OBJ_ATTR)

  
  size_t dset_count = H5Fget_obj_count(H5F_OBJ_ALL,H5F_OBJ_DATASET);
  access->ndsets = dset_count;


  hid_t dset_id_list[dset_count];
  hid_t dset_id;

  if((H5Fget_obj_ids(H5F_OBJ_ALL, H5F_OBJ_DATASET, dset_count, dset_id_list)) < 0){
    printf("- H5Fget_obj_ids : Error\n");
  } else {
    for (int i=0; i< dset_count; i++){

      if(i ==0){
        access->dsets = malloc(sizeof(Dset_access_t ));
      } else {
        access->dsets = realloc(access->dsets, sizeof(Dset_access_t));
      }
      

      dset_id = dset_id_list[i];
      hid_t space_id = H5Dget_space(dset_id);

      // get dataset name
      H5Iget_name(dset_id,access->dsets[i].dset_name, sizeof(access->dsets[i].dset_name));

      // get offset
      access->dsets[i].dset_offset = H5Dget_offset(dset_id);

      // get dimension
      int d = H5Sget_simple_extent_ndims(space_id);

      hsize_t 	dims[d];
      hsize_t 	maxdims[d];
      H5Sget_simple_extent_dims(space_id,dims,maxdims);

      //  H5Sget_simple_extent_npoints
      access->dsets[i].dset_npoints = H5Sget_simple_extent_npoints(space_id);
      access->dsets[i].dset_ndim = d;
      access->dsets[i].dset_dim = (hsize_t *) malloc( d * sizeof(dims));
      access->dsets[i].dset_dim = dims;

      //print_dset_info(dset_id_list[i]);

      // // H5Dget_num_chunks : gives error, stack overflow
      // hsize_t   nchunks;
      // H5Dget_num_chunks(dset_id, H5S_ALL, &nchunks);
      // printf("H5Dget_num_chunks : %zu\n", nchunks);


      // // H5Dget_chunk_info_by_coord
      // unsigned * 	filter_mask;
      // haddr_t * 	addr;
      // hsize_t * 	size;
      // haddr_t   offset = H5Dget_offset(dset_id);
      // H5Dget_chunk_info_by_coord(dset_id, &offset, filter_mask, addr,size);
      // printf("H5Dget_chunk_info_by_coord : filter_mask=%ld addr=%zu size=%zu\n", 
      //   filter_mask, addr, size);

    }
  }

}

/* candice added, for printout file info */
void print_file_info(hid_t file_id){

  // // H5F_OBJ_FILE
  // H5Fget_name: only getting file name...
  char name_buff[100];

  if((H5Fget_name(file_id,name_buff, 100*sizeof(char))) < 0){
    printf("Filename : Error\n");
  } else {
    printf("Filename : %s\n", name_buff); 
  }

  // H5Fget_filesize
  // printf("- file_id : %zu\n", file_id);
  hsize_t filesize;
  if((H5Fget_filesize( file_id, &filesize)) < 0){
    printf("Filesize : Error\n");
  } else {
    printf("Filesize : %zu\n", filesize);
  }

  // H5Dget_access_plist
  // printf("H5Fget_access_plist : %zu\n", H5Fget_access_plist(file_id));

  // printf("\n");
}

void record_all_file(File_access_t * access){

  ssize_t file_count = H5Fget_obj_count(H5F_OBJ_ALL,H5F_OBJ_FILE);
  hid_t file_id_list[file_count];
  if((H5Fget_obj_ids(H5F_OBJ_ALL, H5F_OBJ_FILE, file_count, file_id_list)) < 0){
    printf("- H5Fget_obj_ids : Error\n");
  } else {
    for (int i=0; i< file_count; i++){
      //print_file_info(file_id_list[i]);

      // H5Fget_name
      H5Fget_name(file_id_list[i],access->filename, sizeof(access->filename));

      // H5Fget_filesize
      H5Fget_filesize(file_id_list[i], &(access->file_size));

    }
  }
}

void print_all_file(){
  ssize_t file_count = H5Fget_obj_count(H5F_OBJ_ALL,H5F_OBJ_FILE);
  hid_t file_id_list[file_count];
  if((H5Fget_obj_ids(H5F_OBJ_ALL, H5F_OBJ_FILE, file_count, file_id_list)) < 0){
    printf("- H5Fget_obj_ids : Error\n");
  } else {
    for (int i=0; i< file_count; i++){
      print_file_info(file_id_list[i]);

    }
  }
}


/* candice added, print/record info H5FD__hermes_open from */
void * print_read_write_info(H5FD_mem_t H5_ATTR_UNUSED type,
                                hid_t H5_ATTR_UNUSED dxpl_id, haddr_t addr,
                                size_t size, char * t, size_t blob_size,
                                struct timespec t_start, struct timespec t_end){
  size_t         start_page_index; /* First page index of tranfer buffer */
  size_t         end_page_index; /* End page index of tranfer buffer */
  size_t         num_pages; /* Number of pages of transfer buffer */
  haddr_t        addr_end = addr+size-1;
  File_access_t * access = malloc(sizeof(File_access_t));
  
  
  start_page_index = addr/blob_size;
  end_page_index = addr_end/blob_size;
  num_pages = end_page_index - start_page_index + 1;

  // printf("dxpl_id : %zu\n",dxpl_id); // \t%zu

  // get file id and print info
  // printf("Printing all current file info: ----------------------------- \n");
  // printf("Access_Type : %s\n",t);
  printf("Time_Start(ns) : %ld\n",(t_start.tv_sec * 1000000 + t_start.tv_nsec));
  printf("Time_End(ns) : %ld\n",(t_end.tv_sec * 1000000 + t_end.tv_nsec));
  printf("Time_Elapsed(ns) : %ld\n",((t_end.tv_sec - t_start.tv_sec) * 1000000 + t_end.tv_nsec - t_start.tv_nsec));
  print_all_file();

  // get dataset id and print info
  // printf("Printing all current dataset info: ----------------------------- \n");
  printf("- Access_Size : %ld\n", size);
  print_mem_type(type);
  // printf("- Access_Region_Mem_Type : %s\n", );
  printf("- Start_Page : %ld\n", start_page_index);
  printf("- End_Page : %ld\n", end_page_index);
  printf("- Number_Pages : %ld\n", num_pages);
  printf("- Start_Address : %ld\n", addr);
  printf("- End_Address : %ld\n", addr_end);
  print_all_dset();

  // H5Dget_access_plist : only for create/open

  /* record and print \access struct start */
  // init_file_access_struct(access);

  // strcpy(access->type,t);
  // record_all_file(access);
  // record_mem_type(type, access);
  
  // access->access_size = size;
  // access->addr_start = addr;
  // access->addr_end = addr_end;
  // access->page_start = start_page_index;
  // access->page_end = end_page_index;
  // access->page_num = num_pages;
  // record_all_dset(access);
  // print_data_access_record(access);

  free(access);
  /* record and print \access struct end */
  
}