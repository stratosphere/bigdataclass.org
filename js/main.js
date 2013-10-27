function main() {

	$( "#show-plan-example" ).click(function() {
	  $( "#plan-example" ).slideToggle( "slow", function() {
	    // Animation complete.
	  });
	});

}