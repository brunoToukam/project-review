# Data Dictionary

## S3 Files

### s3a://your-bucket/path/to/data1.parquet
- **Attribute1**: Description of attribute1
- **Attribute2**: Description of attribute2
- **Attribute3**: Description of attribute3

### s3a://your-bucket/path/to/data2.parquet
- **AttributeA**: Description of attributeA
- **AttributeB**: Description of attributeB

## RDS Tables

### octav1_app.octavt_contrat
- **idctr**: Identifiant technique du contrat (unique)
- **cdetypctr**: code type de contrat (Si le contrat est un contrat d'option ou pas)
- **numctr**: Identifiant fonctionnel du contrat (unique)
- **indscbl**: Indicateur de cessibilité du contrat (1 => contrat est cessible; 0 => contrat non cessible)
- **second**: Indicateur secondaire (1 => exclu le contrat; 0 => on garde contrat)
- **datdbtctr**: Date de début de validité du contrat (par rapport à la date d'effet)
- **datfinctr**: Date fin de validité du contrat
- **numinf**: Identifiant fonctionnel pour l'exterieur (ex: Urights)
- **idtercmplx**: Identifiant du territoire complexe
- **datdbtsys**: Date de début de révision
- **datfinsys**: Date de fin de la révision (nulle pour la révision active)
- **biem**: indicateur contrat biem
- **cdestatutctr**: Exclu les contrats qui sont non documentés ("000") ou annulés ("003")
- **expireffetimmediat**: Cutoff. 1 = contrat applicable (si datfinctr), 0 = non-applicable (1 = oui, 0 = non)
- **idtercess**: Territoire limitant la cessibilité
- **datfincollect**: Surcharge date de fin de validité du contrat
- **optaffl**: Option affilié: 0 ne prend pas la restr sur les oeuvre, 1 le contraire
- **fledit**: Rôles des participants (3 => Editeur, Sous-Editeur; 2 => Sous-Editeur; 1 => Editeur, 0 => Aucune condition; null ou espace => rien)
- **Relationships**: Column1 can be joined with `public.table2.Column3`

### octav1_app.octavt_restrpers
- **idrestctr**: Identifiant de restriction du contrat
- **numpers**: Identifiant de la personne
- **numcatal**: Numéro du catalogue de la personne
- **cdeexignc**: Code exigence (INFO, STRICTE, NONSTRICTE)
- **Relationships**: Column3 can be joined with `public.table1.Column1`
