/*
 * xmlUtils.c
 *
 *  Created on: Mar 17, 2012
 *  Copyright Citrusleaf, 2012
 *      Author: csw
 *
 *  A couple of very handy XML utility functions that
 *  I didn't see in the main libxml2 library.
 */

#include <libxml2/libxml/xmlexports.h>
#include <libxml2/libxml/xmlstring.h>
#include <libxml2/libxml/tree.h>

// get first child with the given name
xmlNode *
xmlNodeGetFirstChild(xmlNode *node, xmlChar *childName)
{
	xmlNode *child = node->children;
	while (child) {
		if (!xmlStrcmp(childName, child->name) && child->type == XML_ELEMENT_NODE) {
			return child;
		}
		child = child->next;
	}

	return NULL;
}


// get next child with the same name
xmlNode *
xmlNodeGetNextChild(xmlNode *curChild)
{
	xmlNode *sib = curChild->next;
	while (sib) {
		if (!xmlStrcmp(sib->name, curChild->name) && sib->type == XML_ELEMENT_NODE) {
			return sib;
		}
		sib = sib->next;
	}

	return NULL;
}


