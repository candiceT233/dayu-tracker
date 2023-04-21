/*
 * Purpose:	The public header file for the pass-through VOL connector 
 * for outputing object access order to yaml file.
 */

#include <time.h>

/************/
/* Typedefs */
/************/
typedef struct FileTracker file_list_t;
typedef struct DsetTracker dset_list_t;


/********************* */
/* Function prototypes */
/********************* */
void yaml_dset_dump(dataset_dlife_info_t * dset_info);
void yaml_file_dump(file_dlife_info_t * file_info);
file_list_t * tracker_newfile(file_dlife_info_t * file_info);
dset_list_t * tracker_newdset(const dataset_dlife_info_t * dset_info);


/********************* */
/* Functions */
/********************* */


void tracker_insert_file(file_list_t** head_ref, file_dlife_info_t * file_info){
    file_list_t * newnode = (file_list_t *)malloc(sizeof(file_list_t));
    newnode->time = get_time_usec();
    newnode->name = (char*) malloc(sizeof(file_info->file_name));
    strcpy(newnode->name,file_info->file_name);

    newnode->sorder_id = file_info->sorder_id;
    newnode->porder_id = file_info->porder_id;

    newnode->next = (*head_ref);

    (*head_ref)    = newnode;
}

/*-------------------------------------------------------------------------
 * Function:    tracker_newfile
 *
 * Purpose:     Add a new node to file list
 *
 * Return:      Success:    newnode
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
file_list_t * tracker_newfile(file_dlife_info_t * file_info){
    file_list_t * newnode = (file_list_t *)malloc(sizeof(file_list_t));
    newnode->time = get_time_usec();
    newnode->name = (char*) malloc(sizeof(file_info->file_name));
    strcpy(newnode->name,file_info->file_name);

    newnode->sorder_id = file_info->sorder_id;
    newnode->porder_id = file_info->porder_id;
    // newnode->dataset = (dset_list_t *)malloc(sizeof(dset_list_t));
    newnode->next = NULL;
    return newnode;
}

dset_list_t * tracker_newdset(const dataset_dlife_info_t * dset_info){
    dset_list_t * newnode = (dset_list_t *)malloc(sizeof(dset_list_t));
    newnode->time = get_time_usec();
    newnode->name = (char*) malloc(sizeof(dset_info->obj_info.name));
    strcpy(newnode->name,dset_info->obj_info.name);
    // newnode->name = dset_info->obj_info.name;
    newnode->pfile_name = (char*) malloc(sizeof(dset_info->pfile_name));
    strcpy(newnode->name,dset_info->pfile_name);
    // newnode->pfile_name = dset_info->pfile_name;

    newnode->sorder_id = dset_info->sorder_id;
    newnode->porder_id = dset_info->porder_id;
    newnode->pfile_sorder_id = dset_info->pfile_sorder_id;
    newnode->pfile_porder_id = dset_info->pfile_porder_id;
    newnode->next = NULL;
    
    return newnode;
}

//find a node with given name
file_list_t * findFile(file_list_t * head, char * pfile_name) {

   //start from the first link
   file_list_t * current = head;

   //if list is empty
   if(current == NULL) {
      return NULL;
   }

   //navigate through list
   while(strcmp(current->name, pfile_name) != 0) {
	
      //if it is last node
      if(current->next == NULL) {
         return NULL;
      } else {
         //go to next link
         current = current->next;
      }
   }      
	
   //if data found, return the current Link
   return current;
}

void print_all_tracker(file_list_t * head){
    file_list_t * current = head;
    printf("Entered print_all_tracker\n");
    while(current != NULL){
        tracker_file_dump(current);
        current = current->next;
    }
}

void tracker_dset_dump(dset_list_t * dset_node)
{
    printf("\tdset:\n");
    printf("\t\tid: %ld_%ld-%ld_%ld\n",
        dset_node->pfile_sorder_id, dset_node->pfile_porder_id,
        dset_node->sorder_id, dset_node->porder_id);
    printf("\t\tname: %s\n", dset_node->name);
    // printf("\t\tsize_in_byte: %ld\n", dset_info->storage_size);
}
void tracker_file_dump(file_list_t * file_ndoe){
    printf("Entered tracker_file_dump\n");
    printf("file:\n");
    printf("\tid: %ld_%ld\n",file_ndoe->sorder_id, file_ndoe->porder_id);
    printf("\tname: %s\n", file_ndoe->name);
}

void yaml_dset_dump(dataset_dlife_info_t * dset_info)
{
    printf("\tdset:\n");
    printf("\t\tid: %ld_%ld-%ld_%ld\n",
        dset_info->pfile_sorder_id, dset_info->pfile_porder_id,
        dset_info->sorder_id, dset_info->porder_id);
    printf("\t\tname: %s\n", dset_info->obj_info.name);
    printf("\t\tsize_in_byte: %ld\n", dset_info->storage_size);
}
void yaml_file_dump(file_dlife_info_t * file_info){

    printf("file:\n");
    printf("\tid: %ld_%ld\n",file_info->sorder_id, file_info->porder_id);
    printf("\tname: %s\n", file_info->file_name);
}