########## Custom 함수 리스트 ###################################################################
rect.hclust.nice = function (tree, k = NULL, which = NULL, x = NULL, h = NULL, border = 2,
                             cluster = NULL,  density = NULL,labels = NULL, ...)
{
    if (length(h) > 1 | length(k) > 1)
        stop("'k' and 'h' must be a scalar")
    if (!is.null(h)) {
        if (!is.null(k))
            stop("specify exactly one of 'k' and 'h'")
        k <- min(which(rev(tree$height) < h))
        k <- max(k, 2)
    }
    else if (is.null(k))
        stop("specify exactly one of 'k' and 'h'")
    if (k < 2 | k > length(tree$height))
        stop(gettextf("k must be between 2 and %d", length(tree$height)),
             domain = NA)
    if (is.null(cluster))
        cluster <- cutree(tree, k = k)
    clustab <- table(cluster)[unique(cluster[tree$order])]
    m <- c(0, cumsum(clustab))
    if (!is.null(x)) {
        if (!is.null(which))
            stop("specify exactly one of 'which' and 'x'")
        which <- x
        for (n in 1L:length(x)) which[n] <- max(which(m < x[n]))
    }
    else if (is.null(which))
        which <- 1L:k
    if (any(which > k))
        stop(gettextf("all elements of 'which' must be between 1 and %d",
                      k), domain = NA)
    border <- rep(border, length.out = length(which))
    labels <- rep(labels, length.out = length(which))
    retval <- list()
    for (n in 1L:length(which)) {
        rect(m[which[n]] + 0.66, par("usr")[3L], m[which[n] +
                                                       1] + 0.33, mean(rev(tree$height)[(k - 1):k]), border = border[n], col = border[n], density = density, ...)
        text((m[which[n]] + m[which[n] + 1]+1)/2, grconvertY(grconvertY(par("usr")[3L],"user","ndc")+0.02,"ndc","user"),labels[n])
        retval[[n]] <- which(cluster == as.integer(names(clustab)[which[n]]))
    }
    invisible(retval)
}

# Multiple plot function
#
# ggplot objects can be passed in ..., or to plotlist (as a list of ggplot objects)
# - cols:   Number of columns in layout
# - layout: A matrix specifying the layout. If present, 'cols' is ignored.
#
# If the layout is something like matrix(c(1,2,3,3), nrow=2, byrow=TRUE),
# then plot 1 will go in the upper left, 2 will go in the upper right, and
# 3 will go all the way across the bottom.
#
multiplot <- function(..., plotlist=NULL, file, cols=1, layout=NULL) {
    require(grid)
    
    # Make a list from the ... arguments and plotlist
    plots <- c(list(...), plotlist)
    
    numPlots = length(plots)
    
    # If layout is NULL, then use 'cols' to determine layout
    if (is.null(layout)) {
        # Make the panel
        # ncol: Number of columns of plots
        # nrow: Number of rows needed, calculated from # of cols
        layout <- matrix(seq(1, cols * ceiling(numPlots/cols)),
                         ncol = cols, nrow = ceiling(numPlots/cols))
    }
    
    if (numPlots==1) {
        print(plots[[1]])
        
    } else {
        # Set up the page
        grid.newpage()
        pushViewport(viewport(layout = grid.layout(nrow(layout), ncol(layout))))
        
        # Make each plot, in the correct location
        for (i in 1:numPlots) {
            # Get the i,j matrix positions of the regions that contain this subplot
            matchidx <- as.data.frame(which(layout == i, arr.ind = TRUE))
            
            print(plots[[i]], vp = viewport(layout.pos.row = matchidx$row,
                                            layout.pos.col = matchidx$col))
        }
    }
}